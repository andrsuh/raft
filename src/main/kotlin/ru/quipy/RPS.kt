package ru.quipy

import ru.quipy.raft.NodeAddress
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Initiated by candidates during elections
 *
 * The RPC includes information about the candidate’s log, and the voter denies its vote if its own log is more up-to-date than that of the candidate.
 */
data class RequestVoteRPCRequest(
    val candidatesTerm: Int,
    val candidateId: NodeAddress,
    /**
     * todo move from there then Raft determines which of two logs is more up-to-date by comparing the
     * index and term of the last entries in the logs. If the logs have last entries with different terms, then the log with the
     * later term is more up-to-date. If the logs end with the same term, then whichever log is longer is more up-to-date.
     */
    val lastLogEntryIndex: Int,
    val lastLogEntryTerm: Int,
)

// RPCs description
// Raft servers communicate using remote procedure calls (RPCs),
// and the basic consensus algorithm requires only two types of RPCs

data class RequestVoteRPCResponse(
    override val node: NodeAddress, // voter
    val currentTerm: Int,
    val voteGranted: Boolean
): NodeResponse

// Initiated by leaders to replicate log entries and to provide a form of heartbeat
// AppendEntries RPCs that carry no log entries serves as HB
data class AppendEntriesRPCRequest(
    val leaderTerm: Int,
    val leaderAddress: NodeAddress,
    /**
     * For follower: Leader includes the index and term of the entry in its log that immediately precedes the new entries.
     * If the follower does not find an entry in its log with the same index and term, then it refuses the new entries.
     *
     * For leader: Whenever AppendEntries returns successfully, the leader knows that the follower’s log is identical
     * to its own log up through the new entries.
     */
    val prevEntryTerm: Int? = null,
    val prevEntryLogIndex: Int? = null,

    val logEntry: LogEntry? = null,
    /**
     * See [LeaderProperties.highestCommittedIndex]
     */
    val leaderHighestCommittedIndex: Int, // Once a follower learns that a log entry is committed, it applies the entry to its local state machine (in log order)

)

data class AppendEntriesRPCResponse(
    /**
     * CurrentTerm, for leader to update itself
     */
    val followerTerm: Int,
    /**
     * True if follower contained entry matching prevLogIndex and prevLogTerm
     */
    val success: Boolean
)

/**
 * RPSs below is out of the RAFT protocol scope
 */

data class DbWriteRequest(
    val command: Command,
    val cameFrom: NodeAddress,
    val timeout: Duration = 5.seconds,
)

data class DbWriteResponse(
    val outcome: DbWriteOutcome,
    val logIndex: Int? = null,
    val passedThroughNodes: List<NodeAddress>,
    val nodesUnreached: List<NodeAddress?> = emptyList()
)

enum class DbWriteOutcome {
    SUCCESS, FAIL
}

interface NodeResponse {
    val node: NodeAddress
}