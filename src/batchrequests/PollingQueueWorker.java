package batchrequests;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * The long running thread pool that will process batches from a single queueAndLock.
 * @param <T> The request type
 */
@Slf4j
@Getter
class PollingQueueWorker<T> {

    @Getter private final QueueAndLock<T> queueAndLock;
    private final ExecutorService executorService;
    private final List<Future> taskFutures;
    @Getter private final BatchWriter<T> batchWriter;
    @Getter private final int batchSize;
    @Getter private final int numPollingThreads;
    @Getter private final long maxBufferTimeMs;

    public PollingQueueWorker(QueueAndLock<T> queueAndLock,
                              BatchWriter<T> batchWriter,
                              int batchSize,
                              int numPollingThreads,
                              long maxBufferTimeMs) {
        this.queueAndLock = queueAndLock;
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
        List<Future> taskFutures = new ArrayList<>(numPollingThreads);
        // We could have used executors more traditionally (by submitting poll tasks), but this would have required
        // some infinite loop doing that anyways.
        this.executorService = Executors.newFixedThreadPool(numPollingThreads);
        for (int i = 0; i < numPollingThreads; i++) {
            Future future = executorService.submit(
                    new PollingQueueTask<T>(queueAndLock.getQueue(), queueAndLock.getLock(), batchWriter, batchSize, maxBufferTimeMs));
            taskFutures.add(future);
        }
        this.taskFutures = Collections.unmodifiableList(taskFutures);
        log.info("Polling subtasks are running");
    }

    /**
     * Stop the tasks from polling for more requests.
     * @param graceTimeMs The time to wait for all tasks to shutdown in milliseconds
     * @return See {@link ExecutorService#awaitTermination(long, TimeUnit)}
     * @throws InterruptedException See {@link ExecutorService#awaitTermination(long, TimeUnit)}
     */
    public boolean shutdown(long graceTimeMs) throws InterruptedException {
        // This stops the executor from accepting any new tasks (which it shouldn't be)
        this.executorService.shutdown();
        // We have to actually cancel the tasks so that they stop running
        for (Future f : taskFutures) {
            f.cancel(true);
        }
        return this.executorService.awaitTermination(graceTimeMs, TimeUnit.MILLISECONDS);

    }

    public List<Future> getTaskFutures() {
        return this.taskFutures;
    }

    public static class PollingQueueWorkerBuilder<T> {
        private final QueueAndLock<T> builderQueueAndLock;
        private final BatchWriter<T> builderBatchWriter;
        private final int builderBatchSize;
        private int builderNumPollingThreads = 1;
        private long builderMaxBufferTimeMs = 1000L;

        public PollingQueueWorkerBuilder(QueueAndLock<T> queueAndLock,
                                         BatchWriter<T> batchWriter,
                                         int batchSize) {
            this.builderQueueAndLock = queueAndLock;
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
            return new PollingQueueWorker<>(builderQueueAndLock, builderBatchWriter, builderBatchSize,
                builderNumPollingThreads, builderMaxBufferTimeMs);
        }
    }
}
