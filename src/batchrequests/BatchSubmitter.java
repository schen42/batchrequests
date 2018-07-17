package batchrequests;

import java.util.List;
import java.util.Queue;

/**
 *
 * Main interface for submitting a record to be batched.  This is class NOT thread-safe.
 *
 * <h2>Developer Notes</h2>
 *
 * <h2>TODOs</h2>:
 * - How-To in Javadocs
 * - Different load balancing strategies
 * - Convenience factories for different sinks
 * - Support for CompletableFuture?  This can be passed in right now, but could be convenient.  However, this will
 *   require all batches to go through an extra processing step to complete each record OR require a user to complete
 *   the futures during the post-processing step (and therefore making it required).
 *
 * @param <T> Type of record
 */
public class BatchSubmitter<T> {

    private List<Queue<T>> queues;
    private int currentIndex;

    public BatchSubmitter(List<Queue<T>> queues) {
        this.queues = queues;
        if (queues == null || queues.size() < 1) {
            throw new IllegalArgumentException("List of queues must be non-empty");
        }
        this.currentIndex = 0;
    }

    private synchronized void moveToNextQueue() {
        currentIndex = (currentIndex + 1) % queues.size();
    }

    /**
     * @param requestItem
     */
    public void put(T requestItem) {
        Queue<T> queue = queues.get(currentIndex);
        synchronized (queue) {
            queue.add(requestItem);
            System.out.println(String.format("Put into queue(%s) %d: %s.  Size is now %d", queue, currentIndex, requestItem, queue.size()));
        }
        moveToNextQueue();
    }
}
