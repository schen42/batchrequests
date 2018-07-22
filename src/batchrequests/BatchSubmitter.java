package batchrequests;

import java.util.List;
import java.util.RandomAccess;

/**
 * Main interface for submitting a record to be batched.  This is class NOT thread-safe.
 * @param <T> Type of record
 */
public class BatchSubmitter<T> {
    /**
     * TODOs:
     * - Different load balancing strategies
     * - Different implementation that takes a list of {@link java.util.concurrent.ConcurrentLinkedQueue} instead of
     *   queue and locks.
     * - Support for CompletableFuture?  This can be passed in right now, but could be convenient.  However, this will
     *   require all batches to go through an extra processing step to complete each record OR require a user to complete
     *   the futures during the post-processing step (and therefore making it required).
     */

    private List<QueueAndLock<T>> queueAndLocks;
    private int currentIndex;

    /**
     * @param queueAndLocks Should be a {@link java.util.RandomAccess} list containing {@link QueueAndLock}
     */
    public BatchSubmitter(List<QueueAndLock<T>> queueAndLocks) {
        this.queueAndLocks = queueAndLocks;
        if (queueAndLocks == null || queueAndLocks.size() < 1) {
            throw new IllegalArgumentException("List of queues must be non-empty");
        }
        if (!(queueAndLocks instanceof RandomAccess)) {
            throw new IllegalArgumentException("The provided queues should be in a RandomAccess list");
        }
        this.currentIndex = 0;
    }

    /**
     * @param requestItem An item to collect into a batch, for later batch writing.
     */
    public synchronized void put(T requestItem) {
        QueueAndLock<T> queueAndLock = queueAndLocks.get(currentIndex);
        queueAndLock.getLock().lock();
        queueAndLock.getQueue().add(requestItem);
        queueAndLock.getLock().unlock();
        currentIndex = (currentIndex + 1) % queueAndLocks.size();
    }
}
