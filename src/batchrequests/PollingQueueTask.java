package batchrequests;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;
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
            List<T> batch = new LinkedList<>();
            sharedQueueLock.lock();
            // If the buffer has more items than the batch size, we take enough to fill the batch
            System.out.println(String.format("Size of queue %s: %d", sharedQueue, sharedQueue.size()));
            if (sharedQueue.size() >= batchSize) {
                System.out.println("Buffer reached of size " + sharedQueue.size() + ", min batch size is " + batchSize);
                for (int i = 0; i < batchSize; i++) {
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
                } catch (Exception e) {
                    log.error("Thread.sleep was interrupted, retrying poll", e);
                    break;
                }
                sharedQueueLock.lock();
                int toTake =  Math.min(sharedQueue.size(), batchSize);
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
        isNotShutdown = false;
    }
}
