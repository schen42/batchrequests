package batchrequests;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.Lock;

/**
 * TODOs:
 * - Documentation
 * - Option to re-drive failure back into the queue (can't re-drive it  result processor because that's a cyclic dependency)
 *   This will require more work including max number of retries and could negatively impact batch success if entire
 *   batch fails due to one non-retryable error.
 * - Handle thread interruption/executor shutdown
 */
@RequiredArgsConstructor
@Slf4j
public class PollingQueueTask<T> implements Runnable {

    private final Queue<T> sharedQueue;
    private final Lock sharedQueueLock;
    private final BatchWriter<T, ?> batchWriter;
    private final int batchSize;
    private final long maxBufferTimeMs;
    private boolean isNotShutdown = true;

    @Override
    public void run() {
        log.info("Polling starting");
        while (isNotShutdown) {
            LinkedList<T> batch = new LinkedList<>();
            sharedQueueLock.lock();
            // If the buffer has more items than the batch size, we take enough to fill the batch
            if (sharedQueue.size() >= batchSize) {
                log.info("Buffer reached");
                for (int i = 0; i < batchSize; i++) {
                    batch.add(sharedQueue.poll());
                }
                sharedQueueLock.unlock();
            // Otherwise, we wait for the pre-configured amount of time and take whatever is in the queue
            // to prevent staleness.
            } else {
                log.info("Buffer not reached, waiting");
                sharedQueueLock.unlock();
                try {
                    // TODO: Is there a more testable way of doing this?
                    Thread.sleep(maxBufferTimeMs);
                } catch (Exception e) {
                    log.error("Thread.sleep was interrupted, retrying poll", e);
                    break;
                }
                sharedQueueLock.lock();
                for (int i = 0; i < sharedQueue.size(); i++) {
                    batch.add(sharedQueue.poll());
                }
                sharedQueueLock.unlock();
            }
            batchWriter.performWrite(batch);
        }
    }

    public void shutdown() {
        isNotShutdown = false;
    }
}
