package batchrequests;

import batchrequests.util.DummyBatchWriteResultProcessor;
import batchrequests.util.DummyBatchWriter;
import batchrequests.util.DummyRequest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class BatchRequestIntegrationTests {

    @Test
    public void testSuccessfulBatchWrites() {
        long bufferTimeMs = 10L;
        DummyBatchWriteResultProcessor mockResultProcessor = new DummyBatchWriteResultProcessor();
        DummyBatchWriter mockWriter = new DummyBatchWriter(mockResultProcessor, false);
        BatchRequestsFactory<DummyRequest, List<Integer>> factory = new BatchRequestsFactory.BatchRequestsFactoryBuilder<>(mockWriter)
                .withBatchSize(5)
                .withNumPollingWorkersPerQueue(1)
                .withNumQueues(1)
                .withMaxBufferTimeMs(bufferTimeMs)
                .withBatchWriteResultProcessor(mockResultProcessor)
                .build();
        BatchSubmitter<DummyRequest> batchSubmitter = factory.getBatchSubmitter();
        List<Future> futures = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            System.out.println("Submitting: " + i);
            CompletableFuture<Void> future = new CompletableFuture<>();
            batchSubmitter.put(new DummyRequest(i, future));
            futures.add(future);
        }

        futures.forEach(future -> {
            try {
                future.get(bufferTimeMs * 5, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                throw new RuntimeException("Future did not complete in time");
            }
        });

        Assert.assertEquals(2, mockResultProcessor.getResults().size());
        Assert.assertEquals(Arrays.asList(0,1,2,3,4), mockResultProcessor.getResults().get(0));
        Assert.assertEquals(Arrays.asList(5,6,7), mockResultProcessor.getResults().get(1));
    }

    @Test
    public void testSuccessfulBatchWritesMultiThreaded() {
    }

}
