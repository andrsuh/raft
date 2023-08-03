package ru.quipy

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.select
import org.slf4j.LoggerFactory
import ru.quipy.LogEntry.Companion.emptyLogEntity
import ru.quipy.Node.ElectionResult.*
import ru.quipy.Node.FollowerReplicationStatus.Succeeded
import ru.quipy.NodeRaftStatus.*
import ru.quipy.RaftProperties.Companion.leaderHeartBeatFrequency
import ru.quipy.raft.*
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

private val logger = LoggerFactory.getLogger("raft")

class Node(
    val me: NodeAddress,
    private val clusterInformation: ClusterInformation,
    private val network: Network,
    executor: ExecutorService,
    val log: Log = Log()
) {

    private val dispatcher: CoroutineDispatcher = executor.asCoroutineDispatcher()
    private val scope = CoroutineScope(dispatcher)

    @Volatile
    private var leaderProps = LeaderProperties(mutableMapOf())

    @Volatile
    var termManager = TermManager()

    private val randomNodePart =
        Random.nextLong((RaftProperties.electionTimeoutBase.inWholeMilliseconds * RaftProperties.electionTimeoutRandomizationRatio).toLong())

    private val replicationAwaiter = SuspendableAwaiter<UUID, Unit, Unit>()
    private val replicationChannel = Channel<Pair<Int, UUID>>(capacity = 100_000)

    @Volatile
    private var lastKnownCommittedIndex = 0

    @Volatile
    private var lastHeartBeatFromMe: Long = 0

    fun shutdown() {
        electionJob.stop()
        networkJob.stop()
        replicationJob.stop()
        heartBeatJob.stop()
        scope.cancel()
    }

    private val electionJob = PeriodicalJob(
        "raft",
        "election",
        executor,
        delayer = Delayer.InvocationRatePreservingDelay(10.milliseconds) // todo sukhoa config
    ) { _, _ ->
        val acceptableHBAbsencePeriod = RaftProperties.electionTimeoutBase.plus(randomNodePart.milliseconds)
        val currentTerm = termManager.getCurrentTerm()
        if (currentTerm.raftStatus != LEADER && acceptableHBAbsencePeriod.hasPassedSince(currentTerm.lastHeartbeatFromLeader)) {
            val (updateSucceeded, electionTerm) = termManager.startNewTermCAS(currentTerm) {
                TermInfo(number = currentTerm.number + 1, votedFor = me, raftStatus = CANDIDATE)
            }

            if (!updateSucceeded) {
                logger.info("[election]-[node-${me.address}]-[election-job]: Interrupting election, term $currentTerm has changed")
                return@PeriodicalJob
            }

            val electionResult = try {
                withTimeout(RaftProperties.electionDurationTimeout.inWholeMilliseconds) {
                    initiateElection(electionTerm)
                }
            } catch (e: TimeoutCancellationException) {
                Timeout
            }

            when (electionResult) {
                is Won -> { // todo sukhoa check if the node was downshifted
                    val lastLogRecord = log.last()
                    val indexToStartReplicationWith = if (lastLogRecord == emptyLogEntity) 0 else lastLogRecord.logIndex
                    leaderProps = LeaderProperties(
                        serversExceptMe().map { it to FollowerInfo(indexToStartReplicationWith) }.toMap(mutableMapOf())
                    )
                    replicationChannel.send(indexToStartReplicationWith to UUID.randomUUID())
                    val updateSucceeded = termManager.tryUpdateWhileTermIs(electionTerm.number) {
                        it.copy(raftStatus = LEADER)
                    }
                    if (updateSucceeded) {
                        logger.info("[election]-[node-${me.address}]-[election-job]: Won the election node $me term ${electionTerm.number}. Voters: ${electionResult.votesForMe}")
                    } else {
                        logger.warn("[election]-[node-${me.address}]-[election-job]: Term has changed while won the election node $me term ${electionTerm.number}.")
                    }
                }
                is Failed -> {
                    val updateSucceeded = termManager.tryUpdateWhileTermIs(electionTerm.number) {
                        it.copy(raftStatus = FOLLOWER)
                    }
                    if (updateSucceeded) {
                        logger.info("[election]-[node-${me.address}]-[election-job]: Failed election $me term ${electionTerm.number}")
                    } else {
                        logger.info("[election]-[node-${me.address}]-[election-job]: Term changed while failed election $me term ${electionTerm.number}")
                    }
                }
                is HigherTermDetected -> {
                    /**
                     * if one server’s current term is smaller than the other’s, then it updates its current term to the larger value.
                     * If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state.
                     */
                    logger.info("[election]-[node-${me.address}]-[election-job]: Failed election, higher term detected ${electionResult.termNumber} on node ${electionResult.nodeWithHigherTerm}. My term was ${electionResult.myTerm}")
                    termManager.tryUpdateWhileConditionTrue({ it.number < electionResult.termNumber }) {
                        TermInfo(number = electionResult.termNumber)
                    }
                }
                is Timeout -> {
                    logger.info("[election]-[node-${me.address}]-[election-job]: Election timeout $me term ${electionTerm.number}")
                    termManager.tryUpdateWhileTermIs(electionTerm.number) {
                        it.copy(raftStatus = FOLLOWER)
                    }
                }
                is RecognizedAnotherLeader -> {
                    logger.info("[election]-[node-${me.address}]-[election-job]: Recognised another leader for term ${electionResult.electionTerm}, leader ${electionResult.termInfo}")
                    termManager.tryUpdateWhileTermIs(electionTerm.number) {
                        it.copy(raftStatus = FOLLOWER)
                    }
                }
            }
            termManager.tryUpdateWhileTermIs(electionTerm.number) {
                it.copy(lastHeartbeatFromLeader = System.currentTimeMillis())
            }
        }
    }

    private val networkJob = PeriodicalJob(
        "raft",
        "network",
        executor,
        delayer = Delayer.InvocationRatePreservingDelay(0.milliseconds)
    ) { _, _ ->
        val netPacket = network.readNextNetPacket(me)
        when (val rpc = netPacket.payload) {
            is AppendEntriesRPCRequest -> {
                if (rpc.logEntry == null) {
                    handleIncomingHeartBeat(netPacket.id, rpc)
                } else {
                    handleAppendRequest(netPacket.id, rpc)
                }
            }
            is RequestVoteRPCRequest -> {
                handleIncomingVoteRequest(netPacket.id, rpc)
            }
            is LogWriteClientRequest -> {
                handleIncomingWriteRequest(netPacket.id, rpc)
            }
            else -> throw IllegalStateException("Unknown net packet type: $netPacket")
        }
    }

    // todo sukhoa shoula be done inder lock for supporting concurrent clients
    private fun handleIncomingWriteRequest(requestId: UUID, rpc: LogWriteClientRequest) = scope.launch {
        logger.info("[write]-[node-${me.address}]-[client-write]: Got by node $me from ${rpc.cameFrom}, reqId $requestId")
        val startedAt = System.currentTimeMillis()
        var currentTerm = termManager.getCurrentTerm()
        try {
            withTimeout(rpc.timeout.inWholeMilliseconds) { // todo sukhoa condifure and refactor
                while (currentTerm.leaderAddress == null) {
                    delay(50)
                    currentTerm = termManager.getCurrentTerm()
                }

                if (currentTerm.raftStatus != LEADER) {
                    // todo sukhoa what if CANDIDATE? leaderAddress is null
                    val forward = LogWriteClientRequest(
                        rpc.command,
                        cameFrom = me,
                        timeout = (rpc.timeout - (startedAt.durationUntilNow()))
                    )

                    val response = network.requestAndWait<LogWriteClientRequest, LogWriteClientResponse>(
                        from = me,
                        to = currentTerm.leaderAddress!!,
                        forward
                    )
                    logger.info("[write]-[node-${me.address}]-[client-write-redirect]: Redirect write to the leader ${currentTerm.leaderAddress!!} and wait, reqId $requestId")
                    network.respond(
                        from = me,
                        requestId,
                        response.copy(passedThroughNodes = response.passedThroughNodes + me)
                    )
                    return@withTimeout
                }
                replicationAwaiter.placeKey(requestId)
                val index = log.append(currentTerm.number, rpc.command)
                logger.info("[write]-[node-${me.address}]-[appended]: Leader append write to its log $index, reqId $requestId, rpc $rpc")

                replicationChannel.send(index to requestId)
                val result = replicationAwaiter.putFirstValueAndWaitForSecond(requestId, Unit)
                logger.info("[write]-[node-${me.address}]-[replicated]: Leader got replication response for index $index, result $result, reqId $requestId")

                network.respond(
                    me,
                    requestId,
                    LogWriteClientResponse(LogWriteStatus.SUCCESS, passedThroughNodes = listOf(me))
                )
            }
        } catch (e: TimeoutCancellationException) {
            logger.info("[write]-[node-${me.address}]-[client-write-redirect]: Timeout awaiting response from ${currentTerm.leaderAddress}, reqId $requestId")
            network.respond(
                from = me,
                requestId,
                LogWriteClientResponse(status = LogWriteStatus.FAIL, listOf(me))
            )
        }
    }

    @Volatile
    var currentlyReplicatingIndex = AtomicInteger(-1)

    private val replicationJob = PeriodicalJob(
        "raft",
        "replication",
        executor,
        delayer = Delayer.InvocationRatePreservingDelay(0.milliseconds) // todo sukhoa config
    ) { _, _ ->
        val currentTerm = termManager.getCurrentTerm()
        if (currentTerm.raftStatus != LEADER || log.isEmpty()) { // todo sukhoa check term.leader is me AND MAKE EMPTY METHOD FOR LOG!!!
            delay(5) // todo sukhoa
            return@PeriodicalJob
        }

        val (indexToReplicate, reqId) = replicationChannel.receive()
        while (true) {
            val currIndexToReplicate = currentlyReplicatingIndex.get()
            if (currIndexToReplicate >= indexToReplicate) {
                logger.info("[replication]-[node-${me.address}]-[replication-job]: Reject replication as the index $indexToReplicate is smaller than those in replication $currIndexToReplicate, reqId $reqId")
                return@PeriodicalJob
            } else {
                if (currentlyReplicatingIndex.compareAndSet(currIndexToReplicate, indexToReplicate)) break
            }
        }

        logger.info("[replication]-[node-${me.address}]-[replication-job]: Leader starts replicating log entry index $indexToReplicate, reqId $reqId")

        val replicationJobs = leaderProps.followersInfo.map { (node, info) ->
            if (info.catchUpJob != null && !info.catchUpJob.isCompleted) node to info.catchUpJob
            else node to kickOffCatchUpJobAsync(
                node,
                indexToReplicate,
                currentTerm
            ).also { leaderProps.setTheCatchUpJob(node, it) }
        }.toMap(mutableMapOf())

        logger.info("[replication]-[node-${me.address}]-[replication-job]: Launched RPC calls for every cluster node to replicate log index $indexToReplicate, reqId $reqId")

        var followersReplicated = 1 // Me is a follower as well
        while (followersReplicated < clusterInformation.numberOfNodes) {
            val replicationStatus = select<FollowerReplicationStatus> {
                replicationJobs.forEach { (_, job) ->
                    job.onAwait { it }
                }
            }.also {
                replicationJobs.remove(it.follower)
            }

            if (termManager.getCurrentTerm().raftStatus != LEADER) return@PeriodicalJob // todo sukhoa can be done in other way

            when (replicationStatus) {
                is Succeeded -> {
                    logger.info("[replication]-[node-${me.address}]-[replication-job]: Node ${replicationStatus.follower} successfully replication index $indexToReplicate, reqId $reqId")
                    if (replicationStatus.lastReplicatedIndex == indexToReplicate) {
                        followersReplicated++
                        if (followersReplicated >= clusterInformation.majority) {
                            lastKnownCommittedIndex = indexToReplicate
                            val replicatedEntry = log.logEntryOfIndex(indexToReplicate)
                            logger.info("[replication]-[node-${me.address}]-[replication-job]: Log index $indexToReplicate was confirmed by the majority, reqId $reqId, replicated entry: $replicatedEntry")
                            scope.launch {
                                replicationAwaiter.putSecondValueAndWaitForFirst(
                                    reqId,
                                    Unit
                                )
                            }
                            return@PeriodicalJob
                        }
                    } else {
                        logger.info(
                            "[replication]-[node-${me.address}]-[replication-job]: Index ${replicationStatus.lastReplicatedIndex} " +
                                    ", reqId $reqId replicated to follower ${replicationStatus.follower}, but needed $indexToReplicate, kicked off catch up job"
                        )

                        kickOffCatchUpJobAsync(
                            replicationStatus.follower,
                            indexToReplicate,
                            currentTerm
                        ).also {
                            leaderProps.setTheCatchUpJob(replicationStatus.follower, it)
                            replicationJobs[replicationStatus.follower] = it
                        }
                    }
                }
                is FollowerReplicationStatus.Failed -> {
                    logger.info("[replication]-[node-${me.address}]-[replication-job]: Problem detected , reqId $reqId. Follower ${replicationStatus.follower} has term ${replicationStatus.termNumber}, my term is $currentTerm")
                    continue
                }
            }
        }
    }

    private fun kickOffCatchUpJobAsync(
        follower: NodeAddress,
        indexToReplicate: Int,
        currentTerm: TermInfo
    ): Deferred<FollowerReplicationStatus> {
        return scope.async {

            logger.info("[replication]-[node-${me.address}]-[catch-up]: Kicked off catch up job for index $indexToReplicate")

            val replicationResult = replicateValueToFollower(follower, indexToReplicate, currentTerm)
            if (replicationResult.followerTerm > termManager.getCurrentTerm().number) {
                return@async FollowerReplicationStatus.Failed(follower, replicationResult.followerTerm)
            }
            if (replicationResult.success) {
                if (indexToReplicate == currentlyReplicatingIndex.get()) {
                    return@async Succeeded(follower, indexToReplicate)
                } else {
                    val nextIndex = leaderProps.increaseFollowerReplicationIndex(follower)
                    logger.info("[replication]-[node-${me.address}]-[catch-up]: Follower $follower replicate index $indexToReplicate, increasing index $nextIndex")
                    return@async this@Node.kickOffCatchUpJobAsync(follower, nextIndex, currentTerm).await()
                }
            } else {
                val nextIndex = leaderProps.decreaseFollowerReplicationIndex(follower)
                logger.info("[replication]-[node-${me.address}]-[catch-up]: Follower $follower failed to replicate index $indexToReplicate, decreasing index $nextIndex")
                return@async this@Node.kickOffCatchUpJobAsync(follower, nextIndex, currentTerm).await()
            }
        }
    }

    sealed class FollowerReplicationStatus(val follower: NodeAddress) {
        class Succeeded(follower: NodeAddress, val lastReplicatedIndex: Int) : FollowerReplicationStatus(follower)
        class Failed(follower: NodeAddress, val termNumber: Int) : FollowerReplicationStatus(follower)
    }

    private suspend fun replicateValueToFollower(
        follower: NodeAddress,
        index: Int,
        currentTerm: TermInfo
    ): AppendEntriesRPCResponse {
        // todo sukhoa if leader
        val currentEntry = log.logEntryOfIndex(index)
        val prevEntry = log.logEntryOfIndex(index - 1)

        val appendEntriesRPCRequest = AppendEntriesRPCRequest(
            currentTerm.number,
            me,
            prevEntry.termNumber,
            prevEntry.logIndex,
            currentEntry,
            log.lastCommittedIndex.get()
        )

        logger.info("[replication]-[node-${me.address}]-[replication-call]: Launching call to follower $follower to replicate index $index")
        return network.requestAndWait(
            me,
            follower,
            appendEntriesRPCRequest
        )
    }

    private fun handleIncomingHeartBeat(reqId: UUID, rpc: AppendEntriesRPCRequest) = scope.launch {
        val currentTerm = termManager.getCurrentTerm()
        if (rpc.leaderTerm < currentTerm.number) {
            logger.info("[heartbeat]-[node-${me.address}]-[heartbeat-handle]: Rejected HB from ${rpc.leaderAddress} term ${rpc.leaderTerm} My term is higher")
            return@launch
//            network.respond(me, reqId, AppendEntriesRPCResponse(term.number, success = false))
        }
        log.commitIfRequired(rpc.leaderHighestCommittedIndex)

        // if leader term >= current and the election is in progress then we have to stop the election and recognise the new leader
        if (termManager.tryUpdateWhileConditionTrue({ it.number == currentTerm.number && it.leaderAddress == null }) {
                TermInfo(leaderAddress = rpc.leaderAddress, number = rpc.leaderTerm, raftStatus = FOLLOWER)
            }) {
            logger.info("[heartbeat]-[node-${me.address}]-[heartbeat-handle]: Term changed ${rpc}, previous ${termManager}. Leader Assigned")
            return@launch
        }

        if (termManager.tryUpdateWhileConditionTrue({ rpc.leaderTerm > it.number }) {
                TermInfo(leaderAddress = rpc.leaderAddress, number = rpc.leaderTerm, raftStatus = FOLLOWER)
            }) {
            logger.info("[heartbeat]-[node-${me.address}]-[heartbeat-handle]: Term changed ${rpc}, previous ${termManager}. My term is lower")
            return@launch
        }
        logger.info("[heartbeat]-[node-${me.address}]-[heartbeat-handle]: Heartbeat from ${rpc.leaderAddress} term ${rpc.leaderTerm}")

        termManager.tryUpdateWhileTermIs(currentTerm.number) {
            it.copy(lastHeartbeatFromLeader = System.currentTimeMillis())
        }
    }

    private fun handleAppendRequest(reqId: UUID, rpc: AppendEntriesRPCRequest) = scope.launch {
        var currentTerm = termManager.getCurrentTerm()
        if (rpc.leaderTerm < currentTerm.number) {
            logger.info("[replication]-[node-${me.address}]-[append]: Rejected append request from ${rpc.leaderAddress} term ${rpc.leaderTerm} My term is higher")
            network.respond(me, reqId, AppendEntriesRPCResponse(currentTerm.number, success = false))
        }

        if (termManager.tryUpdateWhileConditionTrue({ it.number == currentTerm.number && it.leaderAddress == null }) {
                TermInfo(leaderAddress = rpc.leaderAddress, number = rpc.leaderTerm, raftStatus = FOLLOWER)
            }) {
            logger.info("[heartbeat]-[node-${me.address}]-[append]: Term changed ${rpc}, previous ${termManager}. Leader Assigned")
        }

        if (termManager.tryUpdateWhileConditionTrue({ rpc.leaderTerm > it.number }) {
                TermInfo(leaderAddress = rpc.leaderAddress, number = rpc.leaderTerm, raftStatus = FOLLOWER)
            }) {
            logger.info("[heartbeat]-[node-${me.address}]-[append]: Term changed ${rpc}, previous ${termManager}. My term is lower")
        }

        currentTerm = termManager.getCurrentTerm() // todo sukhoa race condition

        log.commitIfRequired(rpc.leaderHighestCommittedIndex)
        if (!log.eligibleForReplication(rpc.logEntry!!.logIndex)) {
            logger.info("[replication]-[node-${me.address}]-[append]: Rejecting replication. My committed index ${log.lastCommittedIndex.get()}, passed committed ${rpc.leaderHighestCommittedIndex}, log is empty: ${log.isEmpty()}") // todo sukhoa this code should not know about the rejection reason
            network.respond(me, reqId, AppendEntriesRPCResponse(currentTerm.number, success = false))
        }

        val prevLogEntry = log.logEntryOfIndex(rpc.prevEntryLogIndex!!)
        if (prevLogEntry.logIndex == rpc.prevEntryLogIndex && prevLogEntry.termNumber == rpc.prevEntryTerm) {
            log.acceptValueAndDeleteSubsequent(rpc.logEntry!!)
            logger.info("[replication]-[node-${me.address}]-[append]: Prev entries matches. Value $prevLogEntry. Accept replicating value ${rpc.logEntry}")
            network.respond(me, reqId, AppendEntriesRPCResponse(currentTerm.number, success = true))
        } else {
            logger.info("[replication]-[node-${me.address}]-[append]: Prev entries doesnt match. My value $prevLogEntry. Leader ${rpc}}")
            network.respond(me, reqId, AppendEntriesRPCResponse(currentTerm.number, success = false))
        }

        termManager.tryUpdateWhileTermIs(currentTerm.number) {
            it.copy(lastHeartbeatFromLeader = System.currentTimeMillis())
        }
    }

    private val heartBeatJob = PeriodicalJob(
        "raft",
        "network",
        executor,
        delayer = Delayer.InvocationRatePreservingDelay(leaderHeartBeatFrequency)
    ) { _, _ ->
        val currentTerm = termManager.getCurrentTerm()

        if (currentTerm.raftStatus == LEADER && leaderHeartBeatFrequency.hasPassedSince(currentTerm.lastHeartbeatFromLeader)) {

            val appendEntriesRPCRequest = AppendEntriesRPCRequest(
                currentTerm.number,
                me,
                leaderHighestCommittedIndex = log.lastCommittedIndex.get(),
            )
            network.broadcast(me, serversExceptMe(), appendEntriesRPCRequest)
            lastHeartBeatFromMe = System.currentTimeMillis()
        }
    }

    private suspend fun initiateElection(electionTerm: TermInfo): ElectionResult {
        // To begin an election, a follower increments its current term and transitions to candidate state.
        // It then votes for itself and issues RequestVote RPCs in parallel to each of the other servers in the cluster.
        // A candidate continues in this state until one of three things happens:
        //      (a) it wins the election,
        //      (b) another server establishes itself as leader, or
        //      (c) a period of time goes by with no winner. These outcomes are discussed separately in the paragraphs below.
        //
        // A candidate wins an election if it receives votes from a majority of the servers in the full cluster for the same term. // todo sukhoa cluster properties
        // Each server will vote for at most one candidate in a given term, on a first-come-first-served basis (note: Section 5.4 adds an additional restriction on votes).
        // The majority rule ensures that at most one candidate can win the election for a particular term (the Election Safety Property in Figure 3).
        // Once a candidate wins an election, it becomes leader. It then sends heartbeat messages to all of the other servers to establish its authority and prevent new elections.
        // While waiting for votes, a candidate may receive an AppendEntries RPC from another server claiming to be leader.
        // If the leader’s term (included in its RPC) is at least as large as the candidate’s current term, then the candidate recognizes the leader as legitimate and returns to follower state.
        // If the term in the RPC is smaller than the candidate’s current term, then the candidate rejects the RPC and continues in candidate state.
        // The third possible outcome is that a candidate neither wins nor loses the election: if many followers become candidates at the same time,
        // votes could be split so that no candidate obtains a majority. When this happens, each candidate will time out and start a new election
        // by incrementing its term and initiating another round of RequestVote RPCs. However, without extra measures split votes could repeat indefinitely.

        logger.info("[election]-[node-${me.address}]-[election]: Initiating election $me term $electionTerm")

        val myLastLogEntry = log.last()
        val requestVoteRPCRequest = RequestVoteRPCRequest(
            electionTerm.number,
            me,
            myLastLogEntry.logIndex,
            myLastLogEntry.termNumber
        )

        val awaitingRPSs = mutableMapOf<NodeAddress, Deferred<RequestVoteRPCResponse>>()

        for (server in serversExceptMe()) {
            val res = scope.async {
                network.requestAndWait<RequestVoteRPCRequest, RequestVoteRPCResponse>(
                    me, server, requestVoteRPCRequest
                )
            }
            awaitingRPSs[server] = res
        }

        var responded = 1
        val votedForMe = mutableListOf(me)
        while (responded < clusterInformation.numberOfNodes) {
            // While waiting for votes, a candidate may receive an AppendEntries RPC from another server claiming to be
            // leader. If the leader’s term (included in its RPC) is at least as large as the candidate’s current term,
            // then the candidate recognizes the leader as legitimate and returns to follower state. If the term in the
            // RPC is smaller than the candidate’s current term, then the candidate rejects the RPC and continues in candidate state.
            val latestTerm = termManager.getCurrentTerm()
            if (latestTerm.number > electionTerm.number) {
                return RecognizedAnotherLeader(electionTerm.number, latestTerm) // todo sukhoa should we cancel rpcs?
            }

            val voteResp = select<RequestVoteRPCResponse> {
                awaitingRPSs.values.forEach {
                    it.onAwait { it } // todo sukhoa should it be deleted from list then?
                }
            }.also {
                awaitingRPSs.remove(it.voter)
            }

            responded++
            when {
                voteResp.voteGranted -> {
                    votedForMe.add(voteResp.voter)
                    if (votedForMe.size >= clusterInformation.majority) {
                        termManager.tryUpdateWhileTermIs(electionTerm.number) {
                            TermInfo(
                                me,
                                lastHeartbeatFromLeader = System.currentTimeMillis(),
                                number = electionTerm.number
                            )
                        }
                        return Won(votedForMe, responded)
                    }
                }
                voteResp.currentTerm > electionTerm.number -> {
                    logger.info("[election]-[node-${me.address}]-[rpc-request-vote-resp]: Term returned ${voteResp.currentTerm} is higher than mine $electionTerm")
                    return HigherTermDetected(voteResp.currentTerm, voteResp.voter, electionTerm.number)
                }
            }
        }
        return Failed(votedForMe, responded)
    }

    private suspend fun handleIncomingVoteRequest(reqId: UUID, req: RequestVoteRPCRequest) = scope.launch {
        logger.info("[election]-[node-${me.address}]-[rpc-request-vote-req]: Got vote request $reqId $req")

        var currentTerm = termManager.getCurrentTerm()
        if (req.candidatesTerm < currentTerm.number) {
            val resp = RequestVoteRPCResponse(me, currentTerm.number, false)
            logger.info("[election]-[node-${me.address}]-[rpc-request-vote-req]: Vote against ${req.candidateId}, req $reqId, my term is higher")
            network.respond(me, reqId, resp)
        }

        if (termManager.tryUpdateWhileConditionTrue({ req.candidatesTerm > it.number }) {
                TermInfo(number = req.candidatesTerm)
            }) {
            logger.info("[election]-[node-${me.address}]-[rpc-request-vote-req]: Expired term ${currentTerm.number}, update to ${req.candidatesTerm}")
        }
        currentTerm = termManager.getCurrentTerm()

        val myLastLogEntry = log.last()
        val candidatesLogEntry = req.toLogEntry()


        if (termManager.tryUpdateWhileConditionTrue({ currentTerm.number == it.number && it.votedFor == null && myLastLogEntry <= candidatesLogEntry }) {
                it.copy(votedFor = req.candidateId)
            }) {
            val resp = RequestVoteRPCResponse(me, currentTerm.number, true)
            logger.info("[election]-[node-${me.address}]-[rpc-request-vote-req]: Vote for ${req.candidateId}, req $reqId")
            network.respond(me, reqId, resp)

        } else {
            currentTerm = termManager.getCurrentTerm()
            val resp = RequestVoteRPCResponse(me, currentTerm.number, false)
            val reason =
                if (currentTerm.votedFor != null) "Already voted for ${currentTerm.votedFor} in term ${currentTerm.number}, candidate term ${req.candidatesTerm}" else "My log is more fresh"
            logger.info("[election]-[node-${me.address}]-[rpc-request-vote-req]: Vote against ${req.candidateId}, req $reqId. $reason")
            network.respond(me, reqId, resp)
        }
    }

    sealed class ElectionResult {
        class Won(val votesForMe: List<NodeAddress>, val totalResponded: Int) : ElectionResult()
        class Failed(val votesForMe: List<NodeAddress>, val totalResponded: Int) : ElectionResult()
        class HigherTermDetected(val termNumber: Int, val nodeWithHigherTerm: NodeAddress, val myTerm: Int) :
            ElectionResult()

        object Timeout : ElectionResult()
        class RecognizedAnotherLeader(val electionTerm: Int, val termInfo: TermInfo) : ElectionResult()
    }


    private fun serversExceptMe() = clusterInformation.nodes.filter { it != me }
}

data class FollowerInfo(
    val nextIndexToReplicate: Int,
    val catchUpJob: Deferred<Node.FollowerReplicationStatus>? = null,
)

fun RequestVoteRPCRequest.toLogEntry() =
    LogEntry(InternalCommand(), this.lastLogEntryTerm, this.lastLogEntryIndex, false)

interface Command {
    val id: UUID
}

class InternalCommand(override val id: UUID = UUID.randomUUID()) : Command

fun Duration.hasPassedSince(pointInTime: Long) = System.currentTimeMillis() - pointInTime > this.inWholeMilliseconds

fun Long.durationUntilNow() = (System.currentTimeMillis() - this).milliseconds

//data class RpcCallContext(
//    val calledNode: NodeAddress,
//    override val key: CoroutineContext.Key<RpcCallContext> = RpcCallCtxKey
//) : CoroutineContext.Element {
//    companion object {
//        object RpcCallCtxKey : CoroutineContext.Key<RpcCallContext>
//    }
//}