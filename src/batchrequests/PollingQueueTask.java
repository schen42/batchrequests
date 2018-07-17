package batchrequests;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.locks.Lock;

/**
 * The long-running task that will continuously batch requests and submit the batch for writing.
 * TODOs:
 * - Option to re-drive failure back into the queue (can't re-drive it in result processor because that's a cyclic dependency)
 *   This will require more work including max number of retries and could negatively impact batch success if entire
 *   batch fails due to one non-retryable error.
 * - Handle thread interruption/executor shutdown
 */
@RequiredArgsConstructor
@Slf4j
class PollingQueueTask<T> implements Runnable {

    private final Queue<T> sharedQueue;
    private final Lock sharedQueueLock;
    private final BatchWriter<T> batchWriter;
    private final int maxBatchSize;
    private final long maxBufferTimeMs;
    private boolean shouldContinueProcessing = true;

    /**
     * Run the batch processing, which batches requests in the queue and submits them when the {@link #maxBatchSize}
     * is reached, or when it has waited too long for a batch (defined by {@link #maxBatchSize}).
     */
    @Override
    public void run() {
        log.info("Polling starting");
        while (!Thread.currentThread().isInterrupted() && shouldContinueProcessing) {
            List<T> batch = new LinkedList<>();
            sharedQueueLock.lock();
            // If the buffer has more items than the batch size, we take enough to fill the batch
            System.out.println(String.format("Size of queue %s: %d", sharedQueue, sharedQueue.size()));
            if (sharedQueue.size() >= maxBatchSize) {
                System.out.println("Buffer reached of size " + sharedQueue.size() + ", min batch size is " + maxBatchSize);
                for (int i = 0; i < maxBatchSize; i++) {
                    batch.add(sharedQueue.poll());
                }
                sharedQueueLock.unlock();
            // Otherwise, we wait for the pre-configured amount of time and take whatever is in the queue
            // to prevent staleness.
            } else {
                System.out.println(String.format("Buffer not reached, got %d elements, waiting", sharedQueue.size()));
                sharedQueueLock.unlock();
                try {
                    // TODO: Is there a more testable way of doing this?
                    System.out.println("Sleeping: " + maxBufferTimeMs);
                    Thread.sleep(maxBufferTimeMs);
                } catch (InterruptedException e) {
                    // It appears that a thread throwing an InterruptedException doesn't set the interrupted flag
                    // We could interrupt ourselves, but that seems a little confusing.
                    // Instead, we'll following these docs and manage the thread lifecycle ourselves:
                    // https://docs.oracle.com/javase/8/docs/technotes/guides/concurrency/threadPrimitiveDeprecation.html
                    shouldContinueProcessing = false;
                    log.error("Thread.sleep was interrupted, flushing last batch and killing poller", e);
                }
                sharedQueueLock.lock();
                int toTake =  Math.min(sharedQueue.size(), maxBatchSize);
                System.out.println("Shared queue size right before buffer flush: " + toTake);
                for (int i = 0; i < toTake; i++) {
                    batch.add(sharedQueue.remove());
                }
                sharedQueueLock.unlock();
            }
            batchWriter.performWrite(batch);
        }
    }

    public void shutdown() {
        this.shouldContinueProcessing = false;
    }

    public boolean isShutdown() {
        return !shouldContinueProcessing;
    }
}
