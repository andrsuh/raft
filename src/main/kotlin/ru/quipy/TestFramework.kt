package ru.quipy

import io.prometheus.client.Counter
import io.prometheus.client.exporter.HTTPServer
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import ru.quipy.NodeRaftStatus.LEADER
import ru.quipy.raft.*
import java.util.concurrent.Executors
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds


private val logger = LoggerFactory.getLogger("testing")
private val executor = Executors.newFixedThreadPool(16)
private val testingScope = CoroutineScope(executor.asCoroutineDispatcher())

val requests: Counter = Counter.build()
    .name("requests_total")
    .help("Total requests.")
    .register()

var server = HTTPServer.Builder()
    .withPort(1234)
    .build()

class TestSetup(
    val numberOfRaftNodes: Int = 5,
    val numberOfClientNodes: Int = 1,
    val numberOfWrites: Int = 15,
    val writesTimeout: Duration = 2.seconds,
    val timeoutForDeadlockDetection: Duration = writesTimeout.plus(300.milliseconds),
    val networkChangingHooks: List<NetworkChangerHook> = listOf(
        randomNodePartitionOrOpposite,
        randomNodePartitionOrOpposite,
        leaderPartitioning,
    )
)

data class TestReportItem<T>(
    val response: T,
    val message: String = "",
)

data class TestReport(
    val succcess: Boolean = true,
    val items: List<TestReportItem<*>>,
    val resultedLog: String = "",
    val statesSnapshot: List<String> = listOf(),
) {
    override fun toString(): String {
        return "TestReport(\nsuccess=$succcess,\nresultedLog=${resultedLog.map { "$it" }}),\nitems=${items.map { "\n$it" }}),\nstatesSnapshot=${statesSnapshot.map { "\n$it" }})"
    }
}

fun main() {
    runBlocking {
        (1..1).map {
            testingScope.async {
                val result = test(TestSetup())
                logger.info("Test result: $result")
            }
        }.forEach { it.await() }
    }

    val shutdownNow = executor.shutdownNow()
    logger.warn("Shut down executor, ${shutdownNow.size} task rejected")
    server.close()
}

fun test(setUp: TestSetup): TestReport {
    val testItems = mutableListOf<TestReportItem<*>>()
    val snapshots = mutableListOf<String>()

    val clientNode = NodeAddress(100500) // todo sukhoa use numberOfClientNodes

    val clusterNodesAddresses = (1..(setUp.numberOfRaftNodes)).map { NodeAddress(it) }

    val networkLinks = clusterNodesAddresses.fullyAvailableNetwork()

    val networkTopology = NetworkTopology(networkLinks, listOf(clientNode))

    val network = Network(networkTopology)
    val clusterInformation = ClusterInformation(clusterNodesAddresses)

    val nodes = clusterNodesAddresses.map {
        Node(it, clusterInformation, network, executor)
    }.toList()

    var prevHash = 0;
    val logViewJob = PeriodicalJob(
        "client",
        "log-view",
        executor,
        delayer = Delayer.InvocationRatePreservingDelay(1.milliseconds)
    ) { _, _ ->
        val sb = StringBuilder(500)
        nodes.forEach {
            val currentTerm = it.termManager.getCurrentTerm()
            sb.append(
                "[Node ${it.me}], status: ${if (currentTerm.raftStatus == LEADER) "${currentTerm.raftStatus}  " else currentTerm.raftStatus}, term: ${currentTerm.number}, log: ${
                    it.log.log.map { "| ${it.termNumber}" }.joinToString { it }
                }"
            )
            sb.append("\n")
        }
        val resultString = sb.toString()
        val newHash = resultString.hashCode()
        if (newHash != prevHash) {
            println(resultString)
            snapshots.add(resultString)
            prevHash = newHash
        }
    }

    val numberOfNetChanges = setUp.networkChangingHooks.size
    if (numberOfNetChanges > setUp.numberOfWrites) error("numberOfNetChanges > setUp.numberOfWrites, setup $setUp")

    val networkChangeByOperationNumber = (1..numberOfNetChanges).map { Random.nextInt(setUp.numberOfWrites) }
        .zip(setUp.networkChangingHooks)
        .toMap() // todo sukhoa here the net change may be overwritten as the Randon can return same numbers

    return runBlocking {
        var successfulResponses = 0
        for (i in (1..setUp.numberOfWrites)) {
            try {
                withTimeout(setUp.timeoutForDeadlockDetection.inWholeMilliseconds) {
                    val response = network.requestAndWait<LogWriteClientRequest, LogWriteClientResponse>(
                        clientNode,
                        clusterNodesAddresses.random(),
                        LogWriteClientRequest(InternalCommand(), cameFrom = clientNode, timeout = setUp.writesTimeout)
                    )
                    logger.info("Got response for write operation $response")
                    testItems.add(TestReportItem(response))
                    if (response.status == LogWriteStatus.SUCCESS) {
                        requests.inc()
                        successfulResponses++
                    }
                }
            } catch (e: TimeoutCancellationException) {
                logger.error("Deadlock detected during write operation. Setup $setUp")
                testItems.add(TestReportItem(Unit, message = "Deadlock detected during write operation. Setup $setUp"))
            }

            networkChangeByOperationNumber[i]?.change(nodes, networkLinks)?.also {
                testItems.add(it)
            }
        }

        val resultedLogs = nodes
            .map { it.log.logAsTermsString(successfulResponses) }
            .groupingBy { it }
            .eachCount()
            .filter { it.value >= clusterInformation.majority }

        if (resultedLogs.size > 1) {
            logger.error("Got more than one version of the log $resultedLogs")
            return@runBlocking TestReport(
                succcess = false,
                items = testItems,
                resultedLog = resultedLogs.toString(),
                statesSnapshot = snapshots
            )
        }

        return@runBlocking if (resultedLogs.isEmpty()) {
            logger.error("Didn't get any log $resultedLogs")
            TestReport(succcess = false, items = testItems, statesSnapshot = snapshots)
        } else {
            return@runBlocking resultedLogs.firstNotNullOf {
                if (it.key.length == successfulResponses) {
                    logger.info("Test successfully passed ${it.key}")
                    TestReport(items = testItems, resultedLog = it.key, statesSnapshot = snapshots)
                } else {
                    logger.error("TEST FAIL: LOG ${it.key}, length ${it.key.length}, expected $successfulResponses")
                    TestReport(succcess = false, items = testItems, resultedLog = it.key, statesSnapshot = snapshots)
                }
            }
        }
    }.also {
        nodes.forEach { it.shutdown() }
        logViewJob.stop()
    }
}

fun interface NetworkChangerHook {
    fun change(nodes: List<Node>, links: Set<NetworkLink>): TestReportItem<NetworkChangeResult>
}

data class NetworkChangeResult(
    val linksChanged: List<NetworkLink>
) {
    override fun toString(): String {
//        return "Changed links: ${linksChanged.map { "\n$it" }})\n"
        return "Changed ${linksChanged.size} links"
    }
}

val leaderPartitioning = NetworkChangerHook { nodes: List<Node>, links: Set<NetworkLink> ->
    val leaderNode = nodes.find { it.termManager.getCurrentTerm().raftStatus == LEADER }
    logger.info("Partitioning leader node ${leaderNode?.me} from all others")
    val linksChanged = mutableListOf<NetworkLink>()
    links.forEach { link ->
        leaderNode?.let { leader ->
            if (link sourceOrDestinationIs leader.me) {
                when {
                    link sourceIs leader.me -> link.fromActive = false
                    link destinationIs leader.me -> link.toActive = false
                }
                linksChanged.add(link.copy())
            }
        }
    }
    return@NetworkChangerHook TestReportItem(
        NetworkChangeResult(linksChanged),
        message = "Partitioned current leader node ${leaderNode?.me}"
    )
}


val randomNodePartitioning = NetworkChangerHook { nodes: List<Node>, links: Set<NetworkLink> ->
    val nodeToPartition = nodes.random() // todo sukhoa NPE
    logger.info("Partitioning random node ${nodeToPartition.me} from all others")
    val linksChanged = mutableListOf<NetworkLink>()
    links.filter { link -> link sourceOrDestinationIs nodeToPartition.me }
        .forEach { link ->
            when {
                link sourceIs nodeToPartition.me -> link.fromActive = false
                link destinationIs nodeToPartition.me -> link.toActive = false
            }
            linksChanged.add(link.copy())
        }

    return@NetworkChangerHook TestReportItem(
        NetworkChangeResult(linksChanged),
        message = "Node ${nodeToPartition.me} was partitioned"
    )
}

val randomNodePartitionOrOpposite = NetworkChangerHook { nodes: List<Node>, links: Set<NetworkLink> ->
    val nodeToPartition = nodes.random() // todo sukhoa NPE
    val activate = links
        .filter { link -> link sourceOrDestinationIs nodeToPartition.me }
        .any { link ->
            (link sourceIs nodeToPartition.me && !link.fromActive) ||
                    (link destinationIs nodeToPartition.me && !link.toActive)
        }

    val linksChanged = mutableListOf<NetworkLink>()
    logger.info("${if (activate) "Getting back to cluster" else "Partitioning"} random node ${nodeToPartition.me}")
    links.filter { link -> link sourceOrDestinationIs nodeToPartition.me }
        .forEach { link ->
            when {
                link sourceIs nodeToPartition.me -> link.fromActive = activate
                link destinationIs nodeToPartition.me -> link.toActive = activate
            }
            linksChanged.add(link.copy())
        }

    return@NetworkChangerHook TestReportItem(
        NetworkChangeResult(linksChanged),
        message = "Node ${nodeToPartition.me} was ${if (activate) "got back to cluster" else "partitioned"}"
    )
}