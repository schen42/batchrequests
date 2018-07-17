package batchrequests;

import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * TODO
 * Convenience factory to generate a {@link BatchSubmitter}.
 * @param <T>
 */
@Slf4j
public class BatchRequestsFactory<T> {

    private final BatchWriter<T> batchWriter;
    private final List<Queue<T>> queues;
    private final int batchSize;
    private final int numPollingWorkersPerQueue;
    private final long maxBufferTimeMs;

    private final List<PollingQueueWorker<T>> pollingQueueWorkers;
    private final BatchSubmitter<T> batchSubmitter;

    public BatchRequestsFactory(BatchWriter<T> batchWriter,
                                List<Queue<T>> queues,
                                int batchSize,
                                int numPollingWorkersPerQueue,
                                long maxBufferTimeMs) {
        this.batchWriter = batchWriter;
        this.queues = queues;
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
        private List<Queue<T>> builderQueues = new LinkedList<>(Collections.singletonList(new LinkedList<>()));
        private int builderNumPollingWorkersPerQueue = 1;
        private int builderBatchSize = 25;
        private long builderMaxBufferTimeMs = 1000L;

        public BatchRequestsFactoryBuilder(BatchWriter<T> batchWriter) {
            this.builderBatchWriter = batchWriter;
        }

        /** Convenience function */
        public BatchRequestsFactoryBuilder<T> withNumQueues(int numQueues) {
            if (numQueues < 1) {
                throw new IllegalArgumentException("Number of queues must be positive. Got: " + numQueues);
            }
            this.builderQueues = new ArrayList<>(numQueues);
            for (int i = 0; i < numQueues; i++) {
                builderQueues.add(new LinkedList<>());
            }
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
            return new BatchRequestsFactory<>(builderBatchWriter, builderQueues, builderBatchSize,
                    builderNumPollingWorkersPerQueue, builderMaxBufferTimeMs);
        }
    }
}
