package batchrequests;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * TODO
 * Convenience factory to generate a {@link BatchSubmitter}.
 * @param <T>
 */
@Slf4j
public class BatchRequestsFactory<T> {

    @Getter private final BatchWriter<T> batchWriter;
    @Getter private final List<Queue<T>> queues;
    @Getter private final int batchSize;
    @Getter private final int numPollingWorkersPerQueue;
    @Getter private final long maxBufferTimeMs;

    private final List<PollingQueueWorker<T>> pollingQueueWorkers;
    private final BatchSubmitter<T> batchSubmitter;

    /**
     *
     * @param batchWriter A {@link BatchWriter}
     * @param queues A {@link RandomAccess} list of queues, that will be converted into an unmodifiable list
     * @param batchSize Batch size to be validated
     * @param numPollingWorkersPerQueue Number of workers per queue to be validated
     * @param maxBufferTimeMs Buffer time to be validated
     */
    public BatchRequestsFactory(BatchWriter<T> batchWriter,
                                List<Queue<T>> queues,
                                int batchSize,
                                int numPollingWorkersPerQueue,
                                long maxBufferTimeMs) {
        if (queues == null || queues.size() < 1) {
            throw new IllegalArgumentException("Need non-null list that has a positive number of queues");
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
        this.queues = Collections.unmodifiableList(queues);
        this.batchSize = batchSize;
        this.numPollingWorkersPerQueue = numPollingWorkersPerQueue;
        this.maxBufferTimeMs = maxBufferTimeMs;

        this.pollingQueueWorkers = new ArrayList<>(queues.size());
        for (int i = 0; i < queues.size(); i++) {
            PollingQueueWorker<T> workerForQueue =
                    new PollingQueueWorker<>(queues.get(i), batchWriter, batchSize, numPollingWorkersPerQueue, maxBufferTimeMs);
            pollingQueueWorkers.add(workerForQueue);
        }
        this.batchSubmitter = new BatchSubmitter<>(queues);

        log.info("Initialized BatchSubmitter with {} queues and queue workers, each with {} pollers per queue and a {}ms buffer time",
                pollingQueueWorkers.size(), numPollingWorkersPerQueue, maxBufferTimeMs);
    }

    public BatchSubmitter<T> getBatchSubmitter() {
        return batchSubmitter;
    }

    public static class BatchRequestsFactoryBuilder<T> {
        private final BatchWriter<T> builderBatchWriter;
        private List<Queue<T>> builderQueues;
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

        public BatchRequestsFactoryBuilder<T> withQueues(List<Queue<T>> queues) {
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
                this.builderQueues = Collections.singletonList(new LinkedList<>());
            } else if (this.builderQueues != null && this.builderNumQueues != null) {
                throw new IllegalArgumentException("Cannot set both the queues and the number of queues");
            } else if (this.builderNumQueues != null) {
                if (this.builderNumQueues < 1) {
                    throw new IllegalArgumentException("Number of queues must be positive. Got: " + this.builderNumQueues);
                }
                List<Queue<T>> listOfQueues = new ArrayList<>();
                for (int i = 0; i < this.builderNumQueues; i++) {
                    listOfQueues.add(new LinkedList<>());
                }
                this.builderQueues = listOfQueues;
            } // Otherwise, the queue was set (and assumed to have been validated in the setter)
            return new BatchRequestsFactory<>(builderBatchWriter, builderQueues, builderBatchSize,
                    builderNumPollingWorkersPerQueue, builderMaxBufferTimeMs);
        }
    }
}
