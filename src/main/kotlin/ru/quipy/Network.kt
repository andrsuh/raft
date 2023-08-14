package ru.quipy.raft

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.suspendCancellableCoroutine
import ru.quipy.SuspendableAwaiter
import ru.quipy.networkUpperTimeout
import java.util.*
import kotlin.random.Random

data class NodeAddress(
    val address: Int
)

//
data class NetworkTopology(
    val clusterNetLinks: Set<NetworkLink>,
    val clientNodes: List<NodeAddress>
) {
    fun checkAvailability(from: NodeAddress, to: NodeAddress) =
        (from in clientNodes || to in clientNodes) || ((from linkedTo to) in clusterNetLinks && (to linkedTo from) in clusterNetLinks) // todo sukhoa this is really discussible how we handle this for client nodes

//    fun getAllAvailable(from: NodeAddress) = activeClusterNetLinks[from] ?: emptyList()
}

data class NetworkLink(
    val from: NodeAddress,
    val to: NodeAddress,
    @Volatile
    var fromActive: Boolean = true,
    @Volatile
    var toActive: Boolean = true,
)

infix fun NetworkLink.sourceOrDestinationIs(node: NodeAddress) = this.from == node || this.to == node

infix fun NetworkLink.sourceIs(node: NodeAddress) = this.from == node

infix fun NetworkLink.destinationIs(node: NodeAddress) = this.from == node

fun List<NodeAddress>.fullyAvailableNetwork() = flatMap { node -> this.filter { it != node }.map { node linkedTo it }}.toSet()

infix fun NodeAddress.linkedTo(to: NodeAddress) = NetworkLink(this, to)


class Network(
    private val netState: NetworkTopology,
    private val merger: SuspendableAwaiter<UUID, NetPacket<*>, NetPacket<*>> = SuspendableAwaiter()
) {
    private val buffers = netState.clusterNetLinks.map { it.from }.associateWith { Channel<NetPacket<*>>(50_000) }


    suspend fun <Request, Response> requestAndWait(from: NodeAddress, to: NodeAddress, request: Request): Response {
        if (!netState.checkAvailability(from, to)) {
            suspendCancellableCoroutine<Unit> { }
        }
        delay(Random.nextLong(networkUpperTimeout.inWholeMilliseconds)) // todo sukhoa should be configured by test

        val netPacket = NetPacket(payload = request)
        merger.placeKey(netPacket.id)
        buffers[to]?.send(netPacket) ?: throw IllegalStateException("No buffer found to write to $to")
        val response = merger.putFirstValueAndWaitForSecond(netPacket.id, value = netPacket) as NetPacket<Response>
        return response.payload
    }

    suspend fun <Request> broadcast(from: NodeAddress, toNodes: List<NodeAddress>, request: Request) {
        val netPacket = NetPacket(payload = request)
        toNodes.forEach { to ->
            if (netState.checkAvailability(from, to)) {
                buffers[to]?.send(netPacket) ?: throw IllegalStateException("No buffer found to write to $to")
            }
        }
    }

    suspend fun <Response> respond(from: NodeAddress, requestId: UUID, response: Response) {
//        if (!netState.checkAvailability(from, to)) {
//            suspendCancellableCoroutine<Unit> {  }
//        }

        val netPacket = NetPacket(requestId, payload = response)
        merger.putSecondValueAndWaitForFirst(netPacket.id, value = netPacket)
    }

    suspend fun readNextNetPacket(to: NodeAddress): NetPacket<*> {
        return buffers[to]?.receive() ?: throw IllegalStateException("No buffer found to read from $to")
    }
}

data class NetPacket<Payload>(
    val id: UUID = UUID.randomUUID(),
    val payload: Payload
)