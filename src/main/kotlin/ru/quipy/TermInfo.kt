package ru.quipy

import ru.quipy.TermInfo.Companion.initialTermInfo
import ru.quipy.raft.NodeAddress
import ru.quipy.NodeRaftStatus.FOLLOWER
import java.util.concurrent.atomic.AtomicReference

class TermManager {
    @Volatile
    private var currentTerm: AtomicReference<TermInfo> = AtomicReference(initialTermInfo)

    fun getCurrentTerm() = currentTerm.get()

    fun tryUpdateWhileTermIs(termNumber: Int, transform: (current: TermInfo) -> TermInfo): Boolean {
        while (true) {
            val current = currentTerm.get()
            if (current.number != termNumber) return false
            val (success, _) = updateTermCAS(current, transform)
            if (success) return true
        }
    }

    fun tryUpdateWhileConditionTrue(
        condition: (current: TermInfo) -> Boolean,
        transform: (current: TermInfo) -> TermInfo
    ): Boolean {
        while (true) {
            val current = currentTerm.get()
            if (!condition(current)) return false
            val (success, _) = updateTermCAS(current, transform)
            if (success) return true
        }
    }

    fun updateTermCAS(expected: TermInfo, transform: (current: TermInfo) -> TermInfo): Pair<Boolean, TermInfo> {
        val current = currentTerm.get()
        val updated = transform(current)

        if (current != expected) return false to updated // todo sukhoa weird a bit, returns something undefined value
        return (currentTerm.compareAndExchange(expected, updated) == expected) to updated
    }

    fun startNewTermCAS(expected: TermInfo, transform: () -> TermInfo): Pair<Boolean, TermInfo> {
        val current = currentTerm.get()
        val updated = transform()

        if (current != expected) return false to updated // todo sukhoa weird a bit, returns something undefined value
        return (currentTerm.compareAndExchange(expected, updated) == expected) to updated
    }
}

enum class NodeRaftStatus {
    FOLLOWER,
    LEADER,
    CANDIDATE
}

data class TermInfo(
    val leaderAddress: NodeAddress? = null,
    val raftStatus: NodeRaftStatus = FOLLOWER, // When servers start up, they begin as followers

    /**
     * Current terms are exchanged whenever servers communicate;
     * If one server’s current term is smaller than the other’s, then it updates its current term to the larger value.
     * If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state.
     * If a server receives a request with a stale term number, it rejects the request.
     */
    val number: Int,
    val votedFor: NodeAddress? = null, // todo sukhoa should be updated atomically with currentTerm

    /**
     * See [RaftProperties.electionTimeoutBase]
     */
    val lastHeartbeatFromLeader: Long = System.currentTimeMillis(),
) {
    companion object {
        val initialTermInfo = TermInfo(null, FOLLOWER, 0, null, System.currentTimeMillis())
    }
}