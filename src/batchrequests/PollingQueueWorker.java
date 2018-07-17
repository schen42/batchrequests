package batchrequests;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The long running thread pool that will process batches from a single queue.
 * @param <T> The request type
 */
@Slf4j
class PollingQueueWorker<T> {

    private final Queue<T> queue;
    private final ExecutorService executorService;
    private final List<Future> taskFutures;
    private final BatchWriter<T> batchWriter;
    private final int batchSize;
    private final int numPollingThreads;
    private final long maxBufferTimeMs;

    public PollingQueueWorker(Queue<T> queue,
                              BatchWriter<T> batchWriter,
                              int batchSize,
                              int numPollingThreads,
                              long maxBufferTimeMs) {
        this.queue = queue;
        this.batchWriter = batchWriter;
        this.batchSize = batchSize;
        this.numPollingThreads = numPollingThreads;
        this.maxBufferTimeMs = maxBufferTimeMs;
        if (batchSize < 1) {
            throw new IllegalArgumentException("Batch size must be positive.  Got: " + batchSize);
        }
        if (numPollingThreads < 1) {
            throw new IllegalArgumentException("Number of polling threads must be positive. Got: " + numPollingThreads);
        }
        if (maxBufferTimeMs < 1) {
            throw new IllegalArgumentException("Max buffer time must be positive. Got: " + maxBufferTimeMs);
        }
        this.taskFutures = new ArrayList<>(numPollingThreads);
        this.executorService = Executors.newFixedThreadPool(numPollingThreads);
        for (int i = 0; i < numPollingThreads; i++) {
            Future future = executorService.submit(
                    new PollingQueueTask<T>(queue, new ReentrantLock(), batchWriter, batchSize, maxBufferTimeMs));
            taskFutures.add(future);
        }
        log.info("Polling subtasks are running");
    }

    public static class PollingQueueWorkerBuilder<T> {
        private final Queue<T> builderQueue;
        private final BatchWriter<T> builderBatchWriter;
        private final int builderBatchSize;
        private int builderNumPollingThreads = 1;
        private long builderMaxBufferTimeMs = 1000L;

        public PollingQueueWorkerBuilder(Queue<T> queue,
                                         BatchWriter<T> batchWriter,
                                         int batchSize) {
            this.builderQueue = queue;
            this.builderBatchWriter = batchWriter;
            this.builderBatchSize = batchSize;
        }

        public PollingQueueWorkerBuilder<T> setMaxBufferTime(int maxBufferTimeMs) {
            this.builderMaxBufferTimeMs = maxBufferTimeMs;
            return this;
        }

        public PollingQueueWorkerBuilder<T> setNumPollingThreads(int numPollingThreads) {
            this.builderNumPollingThreads = numPollingThreads;
            return this;
        }

        public PollingQueueWorker<T> build() {
            return new PollingQueueWorker<>(builderQueue, builderBatchWriter, builderBatchSize,
                builderNumPollingThreads, builderMaxBufferTimeMs);
        }
    }
}
