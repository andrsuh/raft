package ru.quipy

import ru.quipy.raft.NodeAddress
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

val networkUpperTimeout = 70.milliseconds

class RaftProperties {
    companion object {
        /**
         * Raft uses randomized election timeouts to ensure that split votes are rare and that they are resolved quickly.
         * To prevent split votes in the first place, election timeouts are chosen randomly from a fixed interval (e.g., 150–300ms).
         * This spreads out the servers so that in most cases only a single server will time out; it wins the election and sends heartbeats
         * before any other servers time out. The same mechanism is used to handle split votes. Each candidate restarts its randomized election
         * timeout at the start of an election, and it waits for that timeout to elapse before starting the next election;
         * this reduces the likelihood of another split vote in the new election. Section 9.3 shows that this approach elects a leader rapidly.
         *
         * If a follower receives no communication over a period of time called the election timeout,
         * then it assumes there is no viable leader and begins an election to choose a new leader
         *
         */
        val electionTimeoutBase = 3.seconds

        val electionTimeoutRandomizationRatio = 0.25

        val leaderHeartBeatFrequency = electionTimeoutBase / 5

        val electionDurationTimeout = networkUpperTimeout * 5
    }
}

class ClusterInformation(val nodes: List<NodeAddress>) {
    val numberOfNodes: Int = nodes.size
    val majority: Int = nodes.size / 2 + 1
}