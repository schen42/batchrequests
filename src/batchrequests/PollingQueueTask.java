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
 */
@RequiredArgsConstructor
@Slf4j
class PollingQueueTask<T> extends Thread {

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
            try {
                List<T> batch = new LinkedList<>();
                sharedQueueLock.lock();
                // If the buffer has more items than the batch size, we take enough to fill the batch
                if (sharedQueue.size() >= maxBatchSize) {
                    for (int i = 0; i < maxBatchSize; i++) {
                        batch.add(sharedQueue.poll());
                    }
                    sharedQueueLock.unlock();
                // Otherwise, we wait for the pre-configured amount of time and take whatever is in the queue
                // to prevent staleness.
                } else {
                    sharedQueueLock.unlock();
                    try {
                        // TODO: Is there a more testable way of doing this?
                        // We can't reliably test if we stop when this thread is interrupted during the sleep
                        // We can't reliably test if we stop this thread outside of the sleep
                        Thread.sleep(maxBufferTimeMs);
                    } catch (InterruptedException e) {
                        // We can interrupt the thread here too, but because we may want to shutdown for other reasons,
                        // we'll follow these docs and manage the thread lifecycle ourselves:
                        // https://docs.oracle.com/javase/8/docs/technotes/guides/concurrency/threadPrimitiveDeprecation.html
                        shouldContinueProcessing = false;
                        log.warn("Thread.sleep was interrupted, flushing last batch and killing poller", e);
                    }
                    sharedQueueLock.lock();
                    int toTake = Math.min(sharedQueue.size(), maxBatchSize);
                    for (int i = 0; i < toTake; i++) {
                        batch.add(sharedQueue.remove());
                    }
                    sharedQueueLock.unlock();
                }
                batchWriter.write(batch);
            } catch (Exception e) {
                log.warn("Unexpected exception in polling task.  Make sure your batch writer handles all RuntimeExceptions", e);
            }
        }
    }

    public void shutdown() {
        this.shouldContinueProcessing = false;
    }

    public boolean isShutdown() {
        return !shouldContinueProcessing;
    }
}
