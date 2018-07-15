package batchrequests;

import batchrequests.util.DummyBatchWriteResultProcessor;
import batchrequests.util.DummyBatchWriter;
import batchrequests.util.DummyRequest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class BatchRequestIntegrationTests {

    @Test
    public void testSuccessfulBatchWrites() throws Exception {
        long bufferTimeMs = 1000L;
        DummyBatchWriteResultProcessor mockResultProcessor = new DummyBatchWriteResultProcessor();
        DummyBatchWriter mockWriter = new DummyBatchWriter(mockResultProcessor, false);
        BatchRequestsFactory<DummyRequest, String> factory = new BatchRequestsFactory.BatchRequestsFactoryBuilder<>(mockWriter)
                .withBatchSize(5)
                .withNumPollingWorkersPerQueue(1)
                .withNumQueues(1)
                .withMaxBufferTimeMs(bufferTimeMs)
                .withBatchWriteResultProcessor(mockResultProcessor)
                .build();
        BatchSubmitter<DummyRequest> batchSubmitter = factory.getBatchSubmitter();
        List<Future> futures = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
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

        Assert.assertEquals(mockResultProcessor.getResults().toString(), 2, mockResultProcessor.getResults().size());
        Assert.assertEquals("1,2,3,4,5", mockResultProcessor.getResults().get(0));
        Assert.assertEquals("1,2,3", mockResultProcessor.getResults().get(0));
    }

}
