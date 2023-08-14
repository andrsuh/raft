package ru.quipy

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import ru.quipy.raft.NodeAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import kotlin.math.max

class StateMachine(private val node: NodeAddress, executor: ExecutorService, private val log: Log) {
    companion object {
        private val logger = LoggerFactory.getLogger("state-machine")
    }

    val map = ConcurrentHashMap<String, Int>() // todo sukhao should be closed private
    private var indexToBeApplied: Int = 0
    private var lastKnownCommittedIndex: Int = -1
    private val stateMachineMutex = Mutex()
    private val scope = CoroutineScope(executor.asCoroutineDispatcher())

    suspend fun updateUpToLatest() {  //= scope.launch {
        val lastCommittedIndex = log.lastCommittedIndex.get()
        stateMachineMutex.withLock {
            lastKnownCommittedIndex = max(lastKnownCommittedIndex, lastCommittedIndex)
            while (indexToBeApplied <= lastKnownCommittedIndex) {
                val entry = log.logEntryOfIndex(indexToBeApplied)
                    ?: error("[state-machine]-[node-${node.address}]-[command-application]: Trying to apply null command under index $indexToBeApplied")
                map[entry.command.key] = entry.command.value
                indexToBeApplied++
                logger.info("[state-machine]-[node-${node.address}]-[command-application]: Applied index ${indexToBeApplied - 1}, command ${entry.command}")
            }
        }
    }
}