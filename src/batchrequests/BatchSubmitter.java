package batchrequests;

import java.util.List;
import java.util.Queue;

/**
 * TODOs:
 * - Different balancing strategies
 * - Convenience factories for different sinks
 * - Individual request callback (if the batch write request returns something)
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

    // TODO: Future?
    // BatchWriteResultProcessor is not passed here because it works at batch level. If it cares about a specific request,
    // the "Batch callback" can operate on logical fields, but not at a unique record level.  Less flexible, but
    // also less complicated
    public void put(T requestItem) {
        queues.get(currentIndex++).add(requestItem);
    }
}
