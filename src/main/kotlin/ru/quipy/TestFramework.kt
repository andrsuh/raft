package ru.quipy

import io.prometheus.client.Counter
import io.prometheus.client.exporter.HTTPServer
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import ru.quipy.NodeRaftStatus.LEADER
import ru.quipy.raft.*
import java.util.concurrent.Executors
import kotlin.random.Random.Default.nextInt
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

data class TestSetup(
    val numberOfRaftNodes: Int = 7,
    val numberOfClientNodes: Int = 1,
    val numberOfWrites: Int = 50,
    val writesTimeout: Duration = 2.seconds,
    val timeoutForDeadlockDetection: Duration = writesTimeout.plus(300.milliseconds),
    val networkChangingHooks: List<NetworkChangerHook> = listOf(
        randomNodePartitionOrOpposite,
        leaderPartitioning,
        randomNodePartitionOrOpposite,
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
    val expectedStateMachine: Map<String, Int>,
) {
    override fun toString(): String {
        return "TestReport(\nsuccess=$succcess,\nsuccessfully sent: ${resultedLog.length}\nresultedLog=${resultedLog.map { "$it" }}),\nitems=${items.map { "\n$it" }}),\nstatesSnapshot=${statesSnapshot.map { "\n$it" }}),\n expected state machine $expectedStateMachine"
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

    val clientNode = DatabaseClient.getLocalAddress()

    val clusterNodesAddresses = (1..(setUp.numberOfRaftNodes)).map { NodeAddress(it) }

    val networkLinks = clusterNodesAddresses.fullyAvailableNetwork()

    val networkTopology = NetworkTopology(networkLinks, listOf(clientNode))

    val network = Network(networkTopology)
    val clusterInformation = ClusterInformation(clusterNodesAddresses)

    val nodes = clusterNodesAddresses.map {
        Node(it, clusterInformation, network, executor)
    }.toList()

    val databaseClient = DatabaseClient(clientNode, clusterNodesAddresses, network, setUp.writesTimeout)

    var prevHash = 0;
    val logViewJob = PeriodicalJob(
        "client",
        "log-view",
        executor,
        delayer = Delayer.InvocationRatePreservingDelay(1000.milliseconds)
    ) { _, _ ->
        val resultString = nodes.snapshot().toString()
        val newHash = resultString.hashCode()
        if (newHash != prevHash) {
            //println(resultString)
            snapshots.add(resultString)
            prevHash = newHash
        }
    }

    val numberOfNetChanges = setUp.networkChangingHooks.size
    if (numberOfNetChanges > setUp.numberOfWrites) error("numberOfNetChanges > setUp.numberOfWrites, setup $setUp")

    val networkChangeByOperationNumber = (1..numberOfNetChanges).map { nextInt(setUp.numberOfWrites) }
        .zip(setUp.networkChangingHooks)
        .toMap() // todo sukhoa here the net change may be overwritten as the Randon can return same numbers

    val rawStateMachine = mutableListOf<Triple<String, Int, Int>>()

    return runBlocking {
        var successfulResponses = 0
        while (true) {
            // here we just wait for cluster to choose the leader
            val leader = nodes.find { it.termManager.getCurrentTerm().raftStatus == LEADER }
            if (leader == null) delay(100) else break
        }

        (1..setUp.numberOfWrites).map { i ->
            async {
                try {
                    withTimeout(setUp.timeoutForDeadlockDetection.inWholeMilliseconds) {
                        val key = nextInt(0, 3).toString()
                        val value = nextInt(0, 100)

                        val response = databaseClient.put(key, value)

                        logger.info("Got response for write operation $response")
                        testItems.add(TestReportItem(response, ", set key=$key, value=$value"))

                        if (response.outcome == DbWriteOutcome.SUCCESS) {
                            rawStateMachine.add(Triple(key, value, response.logIndex!!))
                            successfulResponses++
                        }
                    }
                } catch (e: TimeoutCancellationException) {
                    logger.error("Deadlock detected during write operation. Setup $setUp")
                    testItems.add(
                        TestReportItem(
                            Unit,
                            message = "Deadlock detected during write operation. Setup $setUp"
                        )
                    )
                }

                networkChangeByOperationNumber[i]?.change(nodes, networkLinks)?.also {
                    testItems.add(it)
                }
            }
        }.joinAll()

        delay(8_000) // let the cluster replicate the last values

        val stateMachine = rawStateMachine.sortedBy { it.third }.associate { it.first to it.second }

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
                statesSnapshot = snapshots,
                expectedStateMachine = stateMachine
            )
        }

        return@runBlocking if (resultedLogs.isEmpty()) {
            logger.error("Didn't get any log $resultedLogs")
            TestReport(
                succcess = false,
                items = testItems,
                statesSnapshot = snapshots,
                expectedStateMachine = stateMachine
            )
        } else {
            delay(3_000)
            return@runBlocking resultedLogs.firstNotNullOf {
                if (it.key.length == successfulResponses) {
                    logger.info("Test successfully passed ${it.key}")
                    TestReport(
                        items = testItems,
                        resultedLog = it.key,
                        statesSnapshot = snapshots,
                        expectedStateMachine = stateMachine
                    )
                } else {
                    logger.error("TEST FAIL: LOG ${it.key}, length ${it.key.length}, expected $successfulResponses")
                    TestReport(
                        succcess = false,
                        items = testItems,
                        resultedLog = it.key,
                        statesSnapshot = snapshots,
                        expectedStateMachine = stateMachine
                    )
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

fun List<Node>.snapshot(): StringBuilder {
    val sb = StringBuilder(500)
    forEach {
        val currentTerm = it.termManager.getCurrentTerm()
        sb.append(
            "[Node ${it.me}], " +
                    "status: ${if (currentTerm.raftStatus == LEADER) "${currentTerm.raftStatus}  " else currentTerm.raftStatus}, " +
                    "term: ${currentTerm.termNumber}, " +
                    "log: ${
                        it.log.log.map { " ${it.termNumber}: (${it.command.key},${it.command.value})".padEnd(11, ' ') }
                            .joinToString("| ") { it }
                    } State machine ${it.stateMachine.map}"
        )
        sb.append("\n")
    }
    return sb
}