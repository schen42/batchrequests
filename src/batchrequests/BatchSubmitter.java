package batchrequests;

import java.util.List;
import java.util.Queue;

/**
 *
 * Main interface for submitting a record to be batched.  This is class NOT thread-safe.
 *
 * <h2>Developer Notes</h2>
 * Currently, I do not allow a callback to be passed with the put (i.e. a callback that operates on a record and not
 * batch level).  This is because:
 * - I don't see a use for record-level callbacks. If there are certain flags that require some records to be handled
 *   differently than others, it can be passed in with the record and evaluated after the batch is completed.
 * - I've run into issues where callbacks are very expensive and called at a record-level when the can work more
 *   efficiently at a batch level instead (e.g. emit metrics at an aggregate level for the batch).
 * - Complicates the interface and implementation if we want to be type-safe.  For example, we'll need to pass in
 *   a generic with 2 types everywhere if we want a callback to handle an arbitrary result.
 *   Or, we'll to pass around an Exception if we want to support error handling in callbacks.
 * - It can be added as a backwards compatible change
 *
 * <h2>TODOs</h2>:
 * - How-To in Javadocs
 * - Different load balancing strategies
 * - Convenience factories for different sinks
 * - Support for CompletableFuture?  This can be passed in right now, but could be convenient.  However, this will
 *   require all batches to go through an extra processing step to complete each record.
 *
 * @param <T> Type of record
 */
public class BatchSubmitter<T> {
    /**
     *
     */

    private List<Queue<T>> queues;
    private int currentIndex;

    public BatchSubmitter(List<Queue<T>> queues) {
        this.queues = queues;
        if (queues == null || queues.size() < 1) {
            throw new IllegalArgumentException("List of queues must be non-empty");
        }
        this.currentIndex = 0;
    }

    // TODO: Future?
    public void put(T requestItem) {
        queues.get(currentIndex).add(requestItem);
        currentIndex = (currentIndex + 1) % queues.size();
    }
}
