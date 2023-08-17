package ru.quipy

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import ru.quipy.raft.NodeAddress
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/**
 * Conflicting entries in follower logs will be overwritten with entries from the leader’s log.
 *
 * For leader: To bring a follower’s log into consistency with its own, the leader must find the latest log entry where the two
 * logs agree, delete any entries in the follower’s log after that point, and send the follower all of the leader’s
 * entries after that point.
 */
class Log(private val node: NodeAddress) {
    companion object {
        private val logger = LoggerFactory.getLogger("log")
    }

    val log = CopyOnWriteArrayList<LogEntry>() // todo sukhoa private

    /**
     *  Leader keeps track of the highest index it knows to be committed, and it includes that index in future [AppendEntriesRPCRequest] RPCs (including heartbeats)
     */
    @Volatile
    var lastCommittedIndex = AtomicInteger(-1)

    @Volatile
    private var lastIndex = -1

    private val logMutex = Mutex()

    // todo sukhoa probably it's worth to track the current term and decline writes with smaller term
    suspend fun append(term: Int, command: Command, requestId: UUID): Int {
        logMutex.withLock {
            lastIndex++
            log.add(LogEntry(command, term, lastIndex, false, requestId))
            return lastIndex
        }
    }

    fun last() = log.lastOrNull()

    fun lastCommittedIndex(): Int = lastCommittedIndex.get()

    suspend fun commitIfRequired(indexToCommit: Int): Int? {
        logMutex.withLock {
            while (true) {
                if (lastIndex < indexToCommit || log.isEmpty()) {
                    logger.info("[log]-[node-${node.address}]-[commit-if-required]: Didn't committed $indexToCommit Log size ${log.size}, last index $lastIndex")
                    return null
                }
                val myLastCommitted = lastCommittedIndex.get()
                if (myLastCommitted < indexToCommit) {
                    if (lastCommittedIndex.compareAndSet(myLastCommitted, indexToCommit))  {
                        logger.info("[log]-[node-${node.address}]-[commit-if-required]: Committed index $indexToCommit")
                        return myLastCommitted
                    }
                } else {
                    logger.info("[log]-[node-${node.address}]-[commit-if-required]: Didn't committed $indexToCommit, as mine committed $myLastCommitted")
                    return null
                }
            }
        }
    }

    fun logEntryOfIndex(index: Int): LogEntry? {
        if (index < 0 || index > lastIndex) return null
        return log[index]
    }

    suspend fun acceptValueAndDeleteSubsequent(replicating: LogEntry) {
        logMutex.withLock {
            log.removeIf {
                it.logIndex >= replicating.logIndex
            }
            log.add(replicating)
            lastIndex = replicating.logIndex
        }
    }

    fun logAsTermsString(fistNElements: Int) =
        log.take(fistNElements).map { it.termNumber }.joinToString(separator = "")

    fun isEmpty() = log.isEmpty()

    suspend fun eligibleForReplication(index: Int): Boolean {
        logMutex.withLock {
            val eligible = index > lastCommittedIndex.get() || index > lastIndex || log.isEmpty()
            if (!eligible) {
                logger.info("[log]-[node-${node.address}]-[check-if-eligible]: Rejecting. Index $index should not be accepted. " +
                        "My log size ${log.size}, last index $lastIndex, lastCommittedIndex $lastCommittedIndex")
            }
            return eligible
        }
    }
}

data class LogEntry(
    val command: Command, // stores a state machine command
    val termNumber: Int, // term number when the entry was received by the leader
    val logIndex: Int, // index identifying its position in the log
    val committed: Boolean, // The leader decides when it is safe to apply a log entry to the state machines; such an entry is called committed.
    val requestId: UUID // metadata that designates the requestID under which the logEntry was created
) {
    companion object {
        val dummyRequestUUID = UUID.fromString("436eebfa-5a05-445e-9d4d-70fcd7d7eb1a")
        val emptyLogEntity = LogEntry(InternalCommand(), 0, -1, false, dummyRequestUUID)
    }

    operator fun compareTo(other: LogEntry): Int {
        when {
            this.termNumber < other.termNumber -> return -1
            this.termNumber > other.termNumber -> return 1
        }
        return this.logIndex.compareTo(other.logIndex)
    }
}