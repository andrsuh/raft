package ru.quipy

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.cancel
import ru.quipy.raft.NodeAddress

data class LeaderProperties(

    /**
     * The leader maintains a nextIndex for each follower, which is the index of the next log entry the leader will send to that follower.
     * When a leader first comes to power, it initializes all nextIndex values to the index just after the last one in its log.
     *
     * If a follower’s log is inconsistent with the leader’s, the AppendEntries consistency check will fail in the next
     * AppendEntries RPC. After a rejection, the leader decrements nextIndex and retries the AppendEntries RPC.
     * Eventually nextIndex will reach a point where the leader and follower logs match. When this happens,
     * AppendEntries will succeed, which removes any conflicting entries in the follower’s log and appends
     * entries from the leader’s log (if any). Once AppendEntries succeeds, the follower’s log is consistent with the
     * leader’s, and it will remain that way for the rest of the term.
     */
    val followersInfo: MutableMap<NodeAddress, FollowerInfo>,
) {
    fun decreaseFollowerReplicationIndex(follower: NodeAddress): Int {
        val followerInfo = followersInfo[follower] ?: throw IllegalArgumentException("No such follower $follower")
        if (followerInfo.nextIndexToReplicate < 1) throw IllegalArgumentException("Index to replicate already 0, cant decrease, follower $follower")
        val nextIndexToReplicate = followerInfo.nextIndexToReplicate - 1
        followersInfo[follower] = followerInfo.copy(nextIndexToReplicate = nextIndexToReplicate)
        return nextIndexToReplicate
    }

    fun increaseFollowerReplicationIndex(follower: NodeAddress): Int {
        val followerInfo = followersInfo[follower] ?: throw IllegalArgumentException("No such follower $follower")
        val nextIndexToReplicate = followerInfo.nextIndexToReplicate + 1
        followersInfo[follower] = followerInfo.copy(nextIndexToReplicate = nextIndexToReplicate)
        return nextIndexToReplicate
    }

    fun getNextIndexToReplicate(follower: NodeAddress) =
        followersInfo[follower]?.nextIndexToReplicate ?: throw IllegalArgumentException("No such follower $follower")

    fun setTheCatchUpJob(follower: NodeAddress, catchUpJob: Deferred<Node.FollowerReplicationStatus>?) {
        val followerInfo = followersInfo[follower] ?: throw IllegalArgumentException("No such follower $follower")
        followerInfo.catchUpJob?.cancel("New job launched for the follower $follower")
        followersInfo[follower] = followerInfo.copy(catchUpJob = catchUpJob)

    }
}