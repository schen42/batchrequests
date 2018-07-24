package batchrequests;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Convenience factory to generate a {@link BatchSubmitter}.
 *
 * Create a factory using the provided {@link BatchRequestsFactoryBuilder}.
 *
 * @param <T> Type of request
 */
@Slf4j
public class BatchRequestsFactory<T> {

    @Getter private final BatchWriter<T> batchWriter;
    @Getter private final List<QueueAndLock<T>> queueAndLocks;
    @Getter private final int batchSize;
    @Getter private final int numPollingWorkersPerQueue;
    @Getter private final long maxBufferTimeMs;

    private final List<PollingQueueWorker<T>> pollingQueueWorkers;
    private final BatchSubmitter<T> batchSubmitter;

    public static final int DEFAULT_NUM_QUEUES = 1;
    public static final int DEFAULT_NUM_WORKERS_PER_QUEUE = 1;
    public static final int DEFAULT_MAX_BATCH_SIZE = 25;
    public static final long DEFAULT_MAX_BUFFER_TIME_MS = 1000L;

    /**
     * Constructor with validation.
     * @param batchWriter A non-null {@link BatchWriter}
     * @param queueAndLocks A  non-null, non-empty {@link RandomAccess} list of queueAndLocks, that will be converted into an unmodifiable list
     * @param batchSize A positive-valued batch size
     * @param numPollingWorkersPerQueue A positive-valued number of workers per queue
     * @param maxBufferTimeMs A positive-valued buffer time in which a worker will wait before sending a non-full batch
     */
    public BatchRequestsFactory(BatchWriter<T> batchWriter,
                                List<QueueAndLock<T>> queueAndLocks,
                                int batchSize,
                                int numPollingWorkersPerQueue,
                                long maxBufferTimeMs) {
        if (batchWriter == null) {
            throw new IllegalArgumentException("Need a non-null BatchWriter");
        }
        if (queueAndLocks == null || queueAndLocks.size() < 1) {
            throw new IllegalArgumentException("Need non-null list that has a positive number of queueAndLocks");
        }
        if (batchSize < 1) {
            throw new IllegalArgumentException("Need a positive batch size.  Got: " + batchSize);
        }
        if (numPollingWorkersPerQueue < 1) {
            throw new IllegalArgumentException("Need a positive number of polling workers per queue.  Got: " + numPollingWorkersPerQueue);
        }
        if (maxBufferTimeMs < 1) {
            throw new IllegalArgumentException("Need a positive max buffer time.  Got: " + maxBufferTimeMs);
        }

        this.batchWriter = batchWriter;
        this.queueAndLocks = Collections.unmodifiableList(queueAndLocks);
        this.batchSize = batchSize;
        this.numPollingWorkersPerQueue = numPollingWorkersPerQueue;
        this.maxBufferTimeMs = maxBufferTimeMs;

        this.pollingQueueWorkers = new ArrayList<>(queueAndLocks.size());
        for (int i = 0; i < queueAndLocks.size(); i++) {
            PollingQueueWorker<T> workerForQueue =
                    new PollingQueueWorker<>(queueAndLocks.get(i), batchWriter, batchSize, numPollingWorkersPerQueue, maxBufferTimeMs);
            this.pollingQueueWorkers.add(workerForQueue);
        }
        this.batchSubmitter = new BatchSubmitter<>(queueAndLocks);

        log.info("Initialized BatchSubmitter with {} queueAndLocks and queue workers, each with {} pollers per queue and a {}ms buffer time",
                pollingQueueWorkers.size(), numPollingWorkersPerQueue, maxBufferTimeMs);
    }

    /**
     * @return The {@link BatchSubmitter} used to send requests to batch.
     */
    public BatchSubmitter<T> getBatchSubmitter() {
        return batchSubmitter;
    }

    /**
     * A builder with the required parameters as constructor arguments and the optional parameter as builder setters.
     * @param <T> Type of request
     */
    public static class BatchRequestsFactoryBuilder<T> {
        private final BatchWriter<T> builderBatchWriter;
        private int builderNumPollingWorkersPerQueue = DEFAULT_NUM_WORKERS_PER_QUEUE;
        private int builderBatchSize = DEFAULT_MAX_BATCH_SIZE;
        private Integer builderNumQueues;
        private long builderMaxBufferTimeMs = DEFAULT_MAX_BUFFER_TIME_MS;

        public BatchRequestsFactoryBuilder(BatchWriter<T> batchWriter) {
            this.builderBatchWriter = batchWriter;
        }

        /**
         * Requests are sent to queues to be batched.  Increasing the number of queues increases parallelism, but may
         * increase the probability of not reaching the maximum buffer size (and therefore, the number of batch calls
         * made).
         * @param numQueues The number of queues to use.  Defaults to {@link #DEFAULT_NUM_QUEUES}
         * @return {@link BatchRequestsFactoryBuilder}
         */
        public BatchRequestsFactoryBuilder<T> withNumQueues(int numQueues) {
            this.builderNumQueues = numQueues;
            return this;
        }

        /**
         * Sets the number of polling workers per queue.  In the case that requests are batched and executed at a
         * much slower rate than the queue is filled, you can add more parallel workers to batch requests.
         * @param numPollingWorkersPerQueue Defaults to {@link #DEFAULT_NUM_WORKERS_PER_QUEUE}.
         * @return {@link BatchRequestsFactoryBuilder}
         */
        public BatchRequestsFactoryBuilder<T> withNumPollingWorkersPerQueue(int numPollingWorkersPerQueue) {
            this.builderNumPollingWorkersPerQueue = numPollingWorkersPerQueue;
            return this;
        }

        /**
         * @param batchSize The maximum number of requests per batch.  If the size isn't reached, the worker will wait
         *                  with the time set by {@link #withMaxBufferTimeMs(long)}.
         *                  Defaults to {@link #DEFAULT_MAX_BATCH_SIZE}.
         * @return {@link BatchRequestsFactoryBuilder}
         */
        public BatchRequestsFactoryBuilder<T> withBatchSize(int batchSize) {
            this.builderBatchSize = batchSize;
            return this;
        }


        /**
         * @param maxBufferTimeMs The maximum time to wait for a batch to fill to the size set by {@link #withBatchSize(int)}.
         *                        Defaults to {@link #DEFAULT_MAX_BUFFER_TIME_MS}.
         * @return {@link BatchRequestsFactoryBuilder}
         */
        public BatchRequestsFactoryBuilder<T> withMaxBufferTimeMs(long maxBufferTimeMs) {
            this.builderMaxBufferTimeMs = maxBufferTimeMs;
            return this;
        }

        /**
         * @return {@link BatchRequestsFactory} with the provided options.
         */
        public BatchRequestsFactory<T> build() {
            int numQueues = DEFAULT_NUM_QUEUES;
            if (this.builderNumQueues != null) {
                if (this.builderNumQueues < 1) {
                    throw new IllegalArgumentException("Number of queues must be positive. Got: " + this.builderNumQueues);
                }
                numQueues = this.builderNumQueues;
            }

            List<QueueAndLock<T>> listOfQueues = new ArrayList<>();
            for (int i = 0; i < numQueues; i++) {
                listOfQueues.add(new QueueAndLock<>(new LinkedList<>(), new ReentrantLock()));
            }
            return new BatchRequestsFactory<>(builderBatchWriter, listOfQueues, builderBatchSize,
                    builderNumPollingWorkersPerQueue, builderMaxBufferTimeMs);
        }
    }
}
