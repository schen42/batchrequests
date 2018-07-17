package batchrequests;

import batchrequests.util.DummyBatchWriter;
import batchrequests.util.DummyRequest;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
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
        DummyBatchWriter mockWriter = new DummyBatchWriter(false);
        BatchRequestsFactory<DummyRequest> factory = new BatchRequestsFactory.BatchRequestsFactoryBuilder<>(mockWriter)
                .withBatchSize(5)
                .withNumPollingWorkersPerQueue(1)
                .withNumQueues(1)
                .withMaxBufferTimeMs(bufferTimeMs)
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
                future.get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException("Future did not complete in time");
            }
        });

        MatcherAssert.assertThat(mockWriter.getNumWriteInvocations().get(), Matchers.greaterThan(1));
        Assert.assertEquals(Arrays.asList(0,1,2,3,4), mockWriter.getBatchesWritten().get(0));
        Assert.assertEquals(Arrays.asList(5,6,7), mockWriter.getBatchesWritten().get(1));
    }

    @Test
    public void testSuccessfulBatchWritesMultiThreaded() {
    }

}
