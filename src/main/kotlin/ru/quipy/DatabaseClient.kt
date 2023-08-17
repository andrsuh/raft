package ru.quipy

import ru.quipy.raft.Network
import ru.quipy.raft.NodeAddress
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration

class DatabaseClient(
    private val clientAddress: NodeAddress,
    private val dbNodeAddresses: List<NodeAddress>,
    private val network: Network,
    private val opsTimeout: Duration
) {
    companion object {
        private val clientNodeAddressesSequence = AtomicInteger(100500)
        fun getLocalAddress() = NodeAddress(clientNodeAddressesSequence.getAndIncrement())
    }

    suspend fun put(key: String, value: Int): DbWriteResponse {
        val dbCommand = ClientCommand(key = key, value = value)
        return network.requestAndWait(
            clientAddress,
            dbNodeAddresses.random(),
            DbWriteRequest(dbCommand, cameFrom = clientAddress, timeout = opsTimeout)
        )
    }
}

data class ClientCommand(
    override val id: UUID = UUID.randomUUID(),
    override val key: String,
    override val value: Int
) : Command