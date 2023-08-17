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
import ru.quipy.raft.Network
import ru.quipy.raft.NodeAddress
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
    val log: Log = Log(me),
    val stateMachine: StateMachine = StateMachine(me, executor, log)
) {

    private val dispatcher: CoroutineDispatcher = executor.asCoroutineDispatcher()
    private val scope = CoroutineScope(dispatcher)


    @Volatile
    private var leaderProps = LeaderProperties()

    @Volatile
    var termManager = TermManager()

    private val randomNodePart =
        Random.nextLong((RaftProperties.electionTimeoutBase.inWholeMilliseconds * RaftProperties.electionTimeoutRandomizationRatio).toLong())

    private val replicationAwaiter = SuspendableAwaiter<UUID, Unit, Unit>()
    private val replicationChannel = Channel<Int>(capacity = 100_000)

    @Volatile
    private var lastHeartBeatFromMe: Long = 0

    @Volatile
    var currentlyReplicatingIndex = AtomicInteger(-1)

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
                TermInfo(termNumber = currentTerm.termNumber + 1, votedFor = me, raftStatus = CANDIDATE)
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
                    val indexToStartReplicationWith = lastLogRecord?.logIndex
                    leaderProps = LeaderProperties(
                        serversExceptMe().map { it to FollowerInfo(indexToStartReplicationWith ?: 0) }
                            .toMap(mutableMapOf())
                    )
                    replicationChannel.send(indexToStartReplicationWith ?: 0)

                    val updateSucceeded = termManager.tryUpdateWhileTermIs(electionTerm.termNumber) {
                        it.copy(raftStatus = LEADER)
                    }
                    if (updateSucceeded) {
                        logger.info("[election]-[node-${me.address}]-[election-job]: Won the election node $me term ${electionTerm.termNumber}. Voters: ${electionResult.votesForMe}")
                    } else {
                        logger.warn("[election]-[node-${me.address}]-[election-job]: Term has changed while won the election node $me term ${electionTerm.termNumber}.")
                    }
                }
                is Failed -> {
                    val updateSucceeded = termManager.tryUpdateWhileTermIs(electionTerm.termNumber) {
                        it.copy(raftStatus = FOLLOWER)
                    }
                    if (updateSucceeded) {
                        logger.info("[election]-[node-${me.address}]-[election-job]: Failed election $me term ${electionTerm.termNumber}")
                    } else {
                        logger.info("[election]-[node-${me.address}]-[election-job]: Term changed while failed election $me term ${electionTerm.termNumber}")
                    }
                }
                is HigherTermDetected -> {
                    /**
                     * if one server’s current term is smaller than the other’s, then it updates its current term to the larger value.
                     * If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state.
                     */
                    logger.info("[election]-[node-${me.address}]-[election-job]: Failed election, higher term detected ${electionResult.termNumber} on node ${electionResult.nodeWithHigherTerm}. My term was ${electionResult.myTerm}")
                    termManager.tryUpdateWhileConditionTrue({ it.termNumber < electionResult.termNumber }) {
                        TermInfo(termNumber = electionResult.termNumber)
                    }
                }
                is Timeout -> {
                    logger.info("[election]-[node-${me.address}]-[election-job]: Election timeout $me term ${electionTerm.termNumber}")
                    termManager.tryUpdateWhileTermIs(electionTerm.termNumber) {
                        it.copy(raftStatus = FOLLOWER)
                    }
                }
                is RecognizedAnotherLeader -> {
                    logger.info("[election]-[node-${me.address}]-[election-job]: Recognised another leader for term ${electionResult.electionTerm}, leader ${electionResult.termInfo}")
                    termManager.tryUpdateWhileTermIs(electionTerm.termNumber) {
                        it.copy(raftStatus = FOLLOWER)
                    }
                }
            }
            termManager.tryUpdateWhileTermIs(electionTerm.termNumber) {
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
            is DbWriteRequest -> {
                handleIncomingWriteRequest(netPacket.id, rpc)
            }
            else -> throw IllegalStateException("Unknown net packet type: $netPacket")
        }
    }

    private fun handleIncomingWriteRequest(requestId: UUID, rpc: DbWriteRequest) = scope.launch {
        logger.info("[write]-[node-${me.address}]-[client-write]: Got by node $me from ${rpc.cameFrom}, reqId $requestId")
        val startedAt = System.currentTimeMillis()
        var currentTerm = termManager.getCurrentTerm()
        try {
            withTimeout(rpc.timeout.inWholeMilliseconds) { // todo sukhoa configure and refactor
                while (currentTerm.leaderAddress == null) {
                    delay(30)
                    currentTerm = termManager.getCurrentTerm()
                }

                if (currentTerm.raftStatus != LEADER) {
                    // todo sukhoa what if CANDIDATE? leaderAddress is null
                    val forward = DbWriteRequest(
                        rpc.command,
                        cameFrom = me,
                        timeout = (rpc.timeout - (startedAt.durationUntilNow()))
                    )

                    val response = network.requestAndWait<DbWriteRequest, DbWriteResponse>(
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
                val index = log.append(currentTerm.termNumber, rpc.command, requestId)
                logger.info("[write]-[node-${me.address}]-[appended]: Leader append write to its log $index, reqId $requestId, rpc $rpc")

                replicationChannel.send(index)
                val result = replicationAwaiter.putFirstValueAndWaitForSecond(requestId, Unit)
                logger.info("[write]-[node-${me.address}]-[replicated]: Leader got replication response for index $index, result $result, reqId $requestId")

                network.respond(
                    me,
                    requestId,
                    DbWriteResponse(DbWriteOutcome.SUCCESS, logIndex = index, passedThroughNodes = listOf(me))
                )
            }
        } catch (e: TimeoutCancellationException) {
            logger.info("[write]-[node-${me.address}]-[client-write-redirect]: Timeout awaiting response from ${currentTerm.leaderAddress}, reqId $requestId")
            network.respond(
                from = me,
                requestId,
                DbWriteResponse(outcome = DbWriteOutcome.FAIL, passedThroughNodes = listOf(me), nodesUnreached = listOf(currentTerm.leaderAddress))
            )
        }
    }

    private val replicationJob = PeriodicalJob(
        "raft",
        "replication",
        executor,
        delayer = Delayer.InvocationRatePreservingDelay(0.milliseconds) // todo sukhoa config
    ) { _, _ ->
        val currentTerm = termManager.getCurrentTerm()
        if (currentTerm.raftStatus != LEADER || log.isEmpty()) {
            delay(15) // todo sukhoa
            return@PeriodicalJob
        }

        val indexToReplicate = replicationChannel.receive()
        while (true) {
            val currIndexToReplicate = currentlyReplicatingIndex.get()
            if (currIndexToReplicate >= indexToReplicate) {
                logger.info("[replication]-[node-${me.address}]-[replication-job]: Reject replication as the index $indexToReplicate is smaller than those in replication $currIndexToReplicate")
                return@PeriodicalJob
            } else {
                if (currentlyReplicatingIndex.compareAndSet(currIndexToReplicate, indexToReplicate)) break
            }
        }

        val replicatedEntry = log.logEntryOfIndex(indexToReplicate)
            ?: error("[replication]-[node-${me.address}]-[replication-job]: There is no entry to replicate under index $indexToReplicate")

        logger.info("[replication]-[node-${me.address}]-[replication-job]: Leader starts replicating index $indexToReplicate, log entry $replicatedEntry")

        val replicationJobs = leaderProps.followersInfo.map { (node, info) ->
            if (info.catchUpJob != null && !info.catchUpJob.isCompleted) node to info.catchUpJob
            else node to kickOffCatchingUpJobAsync(node, indexToReplicate, currentTerm)
                .also { leaderProps.setTheCatchUpJob(node, it) }
        }.toMap(mutableMapOf())

        logger.info("[replication]-[node-${me.address}]-[replication-job]: Launched RPC calls for every cluster node to replicate log index $indexToReplicate, log entry $replicatedEntry")

        var followersReplicated = 1 // Me is a follower as well
        while (followersReplicated < clusterInformation.numberOfNodes) {
            val replicationStatus = replicationJobs.awaitFirstAndRemoveFromCalls()

            if (termManager.getCurrentTerm().raftStatus != LEADER) return@PeriodicalJob // todo sukhoa can be done in other way

            when (replicationStatus) {
                is Succeeded -> {
                    logger.info("[replication]-[node-${me.address}]-[replication-job]: Node ${replicationStatus.node} successful replication index $indexToReplicate, reqId ${replicatedEntry.requestId}")
                    if (replicationStatus.replicatedIndex == indexToReplicate) {
                        followersReplicated++
                        if (followersReplicated >= clusterInformation.majority) {
                            val previouslyCommitted = log.commitIfRequired(indexToReplicate)
                            stateMachine.updateUpToLatest()

                            if (previouslyCommitted != null && previouslyCommitted >= 0) { // we could replicate more than one entry during the job
                                for (replicatedIndex in previouslyCommitted..indexToReplicate) {
                                    val entry = log.logEntryOfIndex(replicatedIndex)
                                        ?: error("[replication]-[node-${me.address}]-[replication-job]: There is no entry to replicate under index $replicatedIndex")

                                    logger.info("[replication]-[node-${me.address}]-[replication-job]: Log index $replicatedIndex was confirmed by the majority, reqId ${entry.requestId}, replicated entry: $entry")
                                    scope.launch {
                                        replicationAwaiter.putSecondValueAndWaitForFirst(
                                            entry.requestId,
                                            Unit
                                        )
                                    }
                                }
                            }
                            return@PeriodicalJob
                        }
                    } else {
                        logger.info(
                            "[replication]-[node-${me.address}]-[replication-job]: Index ${replicationStatus.replicatedIndex} " +
                                    ", reqId ${replicatedEntry.requestId} replicated to follower ${replicationStatus.node}, " +
                                    "but needed $indexToReplicate, kicked off catch up job"
                        )

                        kickOffCatchingUpJobAsync(
                            replicationStatus.node,
                            indexToReplicate,
                            currentTerm
                        ).also {
                            leaderProps.setTheCatchUpJob(replicationStatus.node, it)
                            replicationJobs[replicationStatus.node] = it
                        }
                    }
                }
                is FollowerReplicationStatus.Failed -> {
                    logger.info("[replication]-[node-${me.address}]-[replication-job]: Problem detected , reqId ${replicatedEntry.requestId}. Follower ${replicationStatus.node} has term ${replicationStatus.termNumber}, my term is $currentTerm")
                    continue
                }
            }
        }
    }

    private fun kickOffCatchingUpJobAsync(
        follower: NodeAddress,
        indexToReplicate: Int,
        currentTerm: TermInfo,
    ): Deferred<FollowerReplicationStatus> {
        return scope.async {

            logger.info("[replication]-[node-${me.address}]-[catch-up]: Kicked off catch up job for index $indexToReplicate for node $follower")
            val nextIndexToReplicate = leaderProps.getNextIndexToReplicate(follower)
            if (nextIndexToReplicate > indexToReplicate) return@async Succeeded(follower, nextIndexToReplicate)

            val replicationResult = replicateValueToFollower(follower, nextIndexToReplicate, currentTerm)

            if (replicationResult.followerTerm > termManager.getCurrentTerm().termNumber) {
                return@async FollowerReplicationStatus.Failed(follower, replicationResult.followerTerm)
            }
            if (replicationResult.success) {
                if (nextIndexToReplicate == indexToReplicate) {
                    leaderProps.increaseFollowerReplicationIndex(follower)
                    return@async Succeeded(follower, indexToReplicate)
                } else {
                    val nextIndex = leaderProps.increaseFollowerReplicationIndex(follower)
                    logger.info("[replication]-[node-${me.address}]-[catch-up]: Follower $follower replicate index $indexToReplicate, increasing index $nextIndex")
                    return@async this@Node.kickOffCatchingUpJobAsync(follower, indexToReplicate, currentTerm).await()
                }
            } else {
                val nextIndex = leaderProps.decreaseFollowerReplicationIndex(follower)
                logger.info("[replication]-[node-${me.address}]-[catch-up]: Follower $follower failed to replicate index $indexToReplicate, decreasing index $nextIndex")
                return@async this@Node.kickOffCatchingUpJobAsync(follower, indexToReplicate, currentTerm).await()
            }
        }
    }

    sealed class FollowerReplicationStatus(override val node: NodeAddress /* node == follower */) : NodeResponse {
        class Succeeded(follower: NodeAddress, val replicatedIndex: Int) : FollowerReplicationStatus(follower)
        class Failed(follower: NodeAddress, val termNumber: Int) : FollowerReplicationStatus(follower)
    }

    private suspend fun replicateValueToFollower(
        follower: NodeAddress,
        index: Int,
        currentTerm: TermInfo
    ): AppendEntriesRPCResponse {
        // todo sukhoa if leader
        if (index < 0) error("[replication]-[node-${me.address}]-[replication-call]: Trying to replicate index $index")

        val currentEntry = log.logEntryOfIndex(index)
            ?: error("[replication]-[node-${me.address}]-[replication-call]: Trying to replicate index $index, but there is no in my log")
        val prevEntry = log.logEntryOfIndex(index - 1) ?: emptyLogEntity

        val appendEntriesRPCRequest = AppendEntriesRPCRequest(
            currentTerm.termNumber,
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
        if (rpc.leaderTerm < currentTerm.termNumber) {
            logger.info("[heartbeat]-[node-${me.address}]-[heartbeat-handle]: Rejected HB from ${rpc.leaderAddress} term ${rpc.leaderTerm} My term is higher")
            return@launch
//            network.respond(me, reqId, AppendEntriesRPCResponse(term.number, success = false))
        }

        log.commitIfRequired(rpc.leaderHighestCommittedIndex)
        stateMachine.updateUpToLatest()

        // if leader term >= current and the election is in progress then we have to stop the election and recognise the new leader
        if (termManager.tryUpdateWhileConditionTrue({ it.termNumber == currentTerm.termNumber && it.leaderAddress == null }) {
                TermInfo(leaderAddress = rpc.leaderAddress, termNumber = rpc.leaderTerm, raftStatus = FOLLOWER)
            }) {
            logger.info("[heartbeat]-[node-${me.address}]-[heartbeat-handle]: Term changed ${rpc}, previous ${termManager}. Leader Assigned")
            return@launch
        }

        if (termManager.tryUpdateWhileConditionTrue({ rpc.leaderTerm > it.termNumber }) {
                TermInfo(leaderAddress = rpc.leaderAddress, termNumber = rpc.leaderTerm, raftStatus = FOLLOWER)
            }) {
            logger.info("[heartbeat]-[node-${me.address}]-[heartbeat-handle]: Term changed ${rpc}, previous ${termManager}. My term is lower")
            return@launch
        }
        logger.info("[heartbeat]-[node-${me.address}]-[heartbeat-handle]: Heartbeat from ${rpc.leaderAddress} term ${rpc.leaderTerm}")

        termManager.tryUpdateWhileTermIs(currentTerm.termNumber) {
            it.copy(lastHeartbeatFromLeader = System.currentTimeMillis())
        }
    }

    private fun handleAppendRequest(reqId: UUID, rpc: AppendEntriesRPCRequest) = scope.launch {
        var currentTerm = termManager.getCurrentTerm()
        if (rpc.leaderTerm < currentTerm.termNumber) {
            logger.info("[replication]-[node-${me.address}]-[append]: Rejected append request from ${rpc.leaderAddress} term ${rpc.leaderTerm} My term is higher")
            network.respond(me, reqId, AppendEntriesRPCResponse(currentTerm.termNumber, success = false))
        }

        if (termManager.tryUpdateWhileConditionTrue({ it.termNumber == currentTerm.termNumber && it.leaderAddress == null }) {
                TermInfo(leaderAddress = rpc.leaderAddress, termNumber = rpc.leaderTerm, raftStatus = FOLLOWER)
            }) {
            logger.info("[heartbeat]-[node-${me.address}]-[append]: Term changed ${rpc}, previous ${termManager}. Leader Assigned")
        }

        if (termManager.tryUpdateWhileConditionTrue({ rpc.leaderTerm > it.termNumber }) {
                TermInfo(leaderAddress = rpc.leaderAddress, termNumber = rpc.leaderTerm, raftStatus = FOLLOWER)
            }) {
            logger.info("[heartbeat]-[node-${me.address}]-[append]: Term changed ${rpc}, previous ${termManager}. My term is lower")
        }

        currentTerm = termManager.getCurrentTerm() // todo sukhoa race condition

        if (!log.eligibleForReplication(rpc.logEntry!!.logIndex)) {
            network.respond(me, reqId, AppendEntriesRPCResponse(currentTerm.termNumber, success = false))
            return@launch
        }

        val prevLogEntry = log.logEntryOfIndex(rpc.prevEntryLogIndex!!) ?: emptyLogEntity
        if (prevLogEntry.logIndex == rpc.prevEntryLogIndex && prevLogEntry.termNumber == rpc.prevEntryTerm) {
            log.acceptValueAndDeleteSubsequent(rpc.logEntry)
            logger.info("[replication]-[node-${me.address}]-[append]: Prev entries matches. Value $prevLogEntry. Accept replicating value ${rpc.logEntry}")
            network.respond(me, reqId, AppendEntriesRPCResponse(currentTerm.termNumber, success = true))

            log.commitIfRequired(rpc.leaderHighestCommittedIndex)
            stateMachine.updateUpToLatest()
        } else {
            logger.info("[replication]-[node-${me.address}]-[append]: Prev entries doesnt match. My value $prevLogEntry. Leader ${rpc}}")
            network.respond(me, reqId, AppendEntriesRPCResponse(currentTerm.termNumber, success = false))
        }

        termManager.tryUpdateWhileTermIs(currentTerm.termNumber) {
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
                currentTerm.termNumber,
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

        val myLastLogEntry = log.last() ?: emptyLogEntity
        val requestVoteRPCRequest = RequestVoteRPCRequest(
            electionTerm.termNumber,
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
            if (latestTerm.termNumber > electionTerm.termNumber) {
                return RecognizedAnotherLeader(electionTerm.termNumber, latestTerm) // todo sukhoa should we cancel rpcs?
            }

            val voteResp = awaitingRPSs.awaitFirstAndRemoveFromCalls()

            responded++
            when {
                voteResp.voteGranted -> {
                    votedForMe.add(voteResp.node)
                    if (votedForMe.size >= clusterInformation.majority) {
                        termManager.tryUpdateWhileTermIs(electionTerm.termNumber) {
                            TermInfo(
                                me,
                                lastHeartbeatFromLeader = System.currentTimeMillis(),
                                termNumber = electionTerm.termNumber
                            )
                        }
                        return Won(votedForMe, responded)
                    }
                }
                voteResp.currentTerm > electionTerm.termNumber -> {
                    logger.info("[election]-[node-${me.address}]-[rpc-request-vote-resp]: Term returned ${voteResp.currentTerm} is higher than mine $electionTerm")
                    return HigherTermDetected(voteResp.currentTerm, voteResp.node, electionTerm.termNumber)
                }
            }
        }
        return Failed(votedForMe, responded)
    }

    private suspend fun handleIncomingVoteRequest(reqId: UUID, req: RequestVoteRPCRequest) = scope.launch {
        logger.info("[election]-[node-${me.address}]-[rpc-request-vote-req]: Got vote request $reqId $req")

        var currentTerm = termManager.getCurrentTerm()
        if (req.candidatesTerm < currentTerm.termNumber) {
            val resp = RequestVoteRPCResponse(me, currentTerm.termNumber, false)
            logger.info("[election]-[node-${me.address}]-[rpc-request-vote-req]: Vote against ${req.candidateId}, req $reqId, my term is higher")
            network.respond(me, reqId, resp)
        }

        if (termManager.tryUpdateWhileConditionTrue({ req.candidatesTerm > it.termNumber }) {
                TermInfo(termNumber = req.candidatesTerm)
            }) {
            logger.info("[election]-[node-${me.address}]-[rpc-request-vote-req]: Expired term ${currentTerm.termNumber}, update to ${req.candidatesTerm}")
        }
        currentTerm = termManager.getCurrentTerm()

        val myLastLogEntry = log.last() ?: emptyLogEntity
        val candidatesLogEntry = req.toLogEntry(reqId)


        if (termManager.tryUpdateWhileConditionTrue({ currentTerm.termNumber == it.termNumber && it.votedFor == null && myLastLogEntry <= candidatesLogEntry }) {
                it.copy(votedFor = req.candidateId)
            }) {
            val resp = RequestVoteRPCResponse(me, currentTerm.termNumber, true)
            logger.info("[election]-[node-${me.address}]-[rpc-request-vote-req]: Vote for ${req.candidateId}, req $reqId")
            network.respond(me, reqId, resp)

        } else {
            currentTerm = termManager.getCurrentTerm()
            val resp = RequestVoteRPCResponse(me, currentTerm.termNumber, false)
            val reason =
                if (currentTerm.votedFor != null) "Already voted for ${currentTerm.votedFor} in term ${currentTerm.termNumber}, candidate term ${req.candidatesTerm}" else "My log is more fresh"
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

fun RequestVoteRPCRequest.toLogEntry(reqId: UUID) =
    LogEntry(InternalCommand(), this.lastLogEntryTerm, this.lastLogEntryIndex, false, reqId)

interface Command {
    val id: UUID
    val key: String
    val value: Int
}

class InternalCommand(
    override val id: UUID = UUID.randomUUID(),
    override val key: String = "",
    override val value: Int = 0
) : Command

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

suspend fun <T : NodeResponse> MutableMap<NodeAddress, Deferred<T>>.awaitFirstAndRemoveFromCalls() = select<T> {
    forEach { (_, job) ->
        job.onAwait { it }
    }
}.also {
    remove(it.node)
}
