package batchrequests;

import lombok.AllArgsConstructor;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.Lock;

/**
 * TODO: metric on failure
 */
@AllArgsConstructor
public class PollingQueueTask<T> implements Runnable {

    private final Queue<T> sharedQueue;
    private final Lock sharedQueueLock;
    private final BatchWriter<T, ?> batchWriter;
    private final int batchSize;
    private final long maxBufferTimeMs;

    @Override
    public void run() {
        while (true) {
            try {
                sharedQueueLock.lock();
                LinkedList<T> batch;
                if (sharedQueue.size() >= batchSize) {
                    batch = new LinkedList<>();
                    for (int i = 0; i < batchSize; i++) {
                        batch.add(sharedQueue.poll());
                    }
                    sharedQueueLock.unlock();
                } else {
                    sharedQueueLock.unlock();
                    Thread.sleep(maxBufferTimeMs);
                    sharedQueueLock.lock();
                    batch = new LinkedList<>();
                    for (int i = 0; i < sharedQueue.size(); i++) {
                        batch.add(sharedQueue.poll());
                    }
                }
                batchWriter.write(batch);
            } catch (InterruptedException e) {
                // LOG
                break;
            }
        }
    }
}
