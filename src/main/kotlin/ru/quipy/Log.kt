package ru.quipy

import ru.quipy.LogEntry.Companion.emptyLogEntity
import ru.quipy.raft.AppendEntriesRPCRequest
import ru.quipy.raft.Command
import ru.quipy.raft.InternalCommand
import java.util.concurrent.atomic.AtomicInteger

/**
 * Conflicting entries in follower logs will be overwritten with entries from the leader’s log.
 *
 * For leader: To bring a follower’s log into consistency with its own, the leader must find the latest log entry where the two
 * logs agree, delete any entries in the follower’s log after that point, and send the follower all of the leader’s
 * entries after that point.
 */
class Log {
    val log = mutableListOf<LogEntry>() // todo sukhoa private

    /**
     *  Leader keeps track of the highest index it knows to be committed, and it includes that index in future [AppendEntriesRPCRequest] RPCs (including heartbeats)
     */
    @Volatile
    var lastCommittedIndex = AtomicInteger(-1)

    @Volatile
    private var lastIndex = -1

    @Synchronized
    fun append(term: Int, command: Command): Int {
        lastIndex++
        log.add(LogEntry(command, term, lastIndex, false))
        return lastIndex
    }

    fun last() = log.lastOrNull() ?: emptyLogEntity // todo sukhoa sync and others too

//    fun lastCommittedIndex(): Int = lastCommittedIndex

    fun commitIfRequired(indexToCommit: Int) {
        while (true) {
            val myLastCommitted = lastCommittedIndex.get()
            if (myLastCommitted < indexToCommit) {
                if (lastCommittedIndex.compareAndSet(myLastCommitted, indexToCommit)) break
            } else break
        }
    }

    fun logEntryOfIndex(index: Int): LogEntry {
        if (index < 0 || index > lastIndex) return emptyLogEntity
        return log[index]
    }

    @Synchronized
    fun acceptValueAndDeleteSubsequent(replicating: LogEntry) {
        log.removeIf {
            it.logIndex >= replicating.logIndex
        }
        log.add(replicating)
        lastIndex = replicating.logIndex
    }

    fun logAsTermsString(fistNElements: Int) =
        log.take(fistNElements).map { it.termNumber }.joinToString(separator = "")

    fun isEmpty() = log.isEmpty()

    fun eligibleForReplication(index: Int) = index > lastCommittedIndex.get() || index > lastIndex || log.isEmpty()
}

data class LogEntry(
    val command: Command, // stores a state machine command
    val termNumber: Int, // term number when the entry was received by the leader
    val logIndex: Int, // index identifying its position in the log
    val committed: Boolean // The leader decides when it is safe to apply a log entry to the state machines; such an entry is called committed.
) {
    companion object {
        val emptyLogEntity = LogEntry(InternalCommand(), 0, -1, false)
    }

    operator fun compareTo(other: LogEntry): Int {
        when {
            this.termNumber < other.termNumber -> return -1
            this.termNumber > other.termNumber -> return 1
        }
        return this.logIndex.compareTo(other.logIndex)
    }
}