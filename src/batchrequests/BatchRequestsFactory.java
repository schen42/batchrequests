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
        private List<QueueAndLock<T>> builderQueues;
        private int builderNumPollingWorkersPerQueue = 1;
        private int builderBatchSize = 25;
        private Integer builderNumQueues;
        private long builderMaxBufferTimeMs = 1000L;

        public BatchRequestsFactoryBuilder(BatchWriter<T> batchWriter) {
            this.builderBatchWriter = batchWriter;
        }

        /** Convenience function */
        public BatchRequestsFactoryBuilder<T> withNumQueues(int numQueues) {
            this.builderNumQueues = numQueues;
            return this;
        }

        public BatchRequestsFactoryBuilder<T> withQueues(List<QueueAndLock<T>> queues) {
            this.builderQueues = queues;
            return this;
        }

        public BatchRequestsFactoryBuilder<T> withNumPollingWorkersPerQueue(int numPollingWorkersPerQueue) {
            this.builderNumPollingWorkersPerQueue = numPollingWorkersPerQueue;
            return this;
        }

        public BatchRequestsFactoryBuilder<T> withBatchSize(int batchSize) {
            this.builderBatchSize = batchSize;
            return this;
        }


        public BatchRequestsFactoryBuilder<T> withMaxBufferTimeMs(long maxBufferTimeMs) {
            this.builderMaxBufferTimeMs = maxBufferTimeMs;
            return this;
        }

        public BatchRequestsFactory<T> build() {
            if (this.builderQueues == null && this.builderNumQueues == null) {
                // By default, have only one queue
                this.builderQueues = Collections.singletonList(new QueueAndLock<>(new LinkedList<>(), new ReentrantLock()));
            } else if (this.builderQueues != null && this.builderNumQueues != null) {
                throw new IllegalArgumentException("Cannot set both the queueAndLocks and the number of queueAndLocks");
            } else if (this.builderNumQueues != null) {
                if (this.builderNumQueues < 1) {
                    throw new IllegalArgumentException("Number of queueAndLocks must be positive. Got: " + this.builderNumQueues);
                }
                List<QueueAndLock<T>> listOfQueues = new ArrayList<>();
                for (int i = 0; i < this.builderNumQueues; i++) {
                    listOfQueues.add(new QueueAndLock<>(new LinkedList<>(), new ReentrantLock()));
                }
                this.builderQueues = listOfQueues;
            } // Otherwise, the queue was set (and assumed to have been validated in the setter)
            return new BatchRequestsFactory<>(builderBatchWriter, builderQueues, builderBatchSize,
                    builderNumPollingWorkersPerQueue, builderMaxBufferTimeMs);
        }
    }
}
