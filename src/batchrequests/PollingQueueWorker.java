package batchrequests;

import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

public class PollingQueueWorker<T> {

    private final LinkedList<T> queue;
    private final ExecutorService executorService;
    private final BatchWriter<T, ?> batchWriter;
    private final int batchSize;
    private final int numPollingThreads;
    private final long maxBufferTimeMs;

    public PollingQueueWorker(LinkedList<T> queue,
                              ExecutorService executorService,
                              BatchWriter<T, ?> batchWriter,
                              int batchSize,
                              int numPollingThreads,
                              long maxBufferTimeMs) {
        this.queue = queue;
        this.executorService = executorService;
        this.batchWriter = batchWriter;
        this.batchSize = batchSize;
        this.numPollingThreads = numPollingThreads;
        this.maxBufferTimeMs = maxBufferTimeMs;
        if (numPollingThreads < 1) {
            throw new IllegalArgumentException("Number of polling threads must be positive. Got: " + numPollingThreads);
        }
        if (maxBufferTimeMs < 1) {
            throw new IllegalArgumentException("Max buffer time must be positive. Got: " + maxBufferTimeMs);
        }
        for (int i = 0; i < numPollingThreads; i++) {
            executorService.submit(new PollingQueueTask<T>(queue, new ReentrantLock(), batchWriter, batchSize, maxBufferTimeMs));
        }
    }

    public static class PollingQueueWorkerBuilder<T> {
        private final LinkedList<T> builderQueue;
        private final ExecutorService builderExecutorService;
        private final BatchWriter<T, ?> builderBatchWriter;
        private final int builderBatchSize;
        private int builderNumPollingThreads = 1;
        private long builderMaxBufferTimeMs = 1000L;

        public PollingQueueWorkerBuilder(LinkedList<T> queue,
                                         BatchWriter<T, ?> batchWriter,
                                         ExecutorService executorService,
                                         int batchSize) {
            this.builderQueue = queue;
            this.builderExecutorService = executorService;
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
            return new PollingQueueWorker<T>(builderQueue, builderExecutorService, builderBatchWriter, builderBatchSize,
                builderNumPollingThreads, builderMaxBufferTimeMs);
        }
    }
}
