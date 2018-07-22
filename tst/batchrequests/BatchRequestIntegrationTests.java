package batchrequests;

import batchrequests.util.DummyBatchWriter;
import batchrequests.util.DummyRequest;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
        int numRecordsToSubmit = 8;
        for (int i = 0; i < numRecordsToSubmit; i++) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            batchSubmitter.put(new DummyRequest(i, future));
            futures.add(future);
        }
        futures.forEach(future -> {
            try {
                future.get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException("Future did not complete in time.  Got: " + mockWriter.getBatchesWritten());
            }
        });

        MatcherAssert.assertThat(mockWriter.getNumWriteInvocations().get(), Matchers.greaterThan(1));
        List<Integer> itemsWritten = mockWriter.getBatchesWritten().stream().flatMap(List::stream).collect(Collectors.toList());
        List<Integer> expectedElements =  IntStream.range(0, numRecordsToSubmit).boxed().collect(Collectors.toList());
        MatcherAssert.assertThat(itemsWritten, Matchers.containsInAnyOrder(expectedElements.toArray()));
    }

    @Test
    public void testSuccessfulBatchWritesMultiThreaded() {
        long bufferTimeMs = 100L;
        DummyBatchWriter mockWriter = new DummyBatchWriter(false);
        BatchRequestsFactory<DummyRequest> factory = new BatchRequestsFactory.BatchRequestsFactoryBuilder<>(mockWriter)
                .withBatchSize(5)
                .withNumPollingWorkersPerQueue(2)
                .withNumQueues(2)
                .withMaxBufferTimeMs(bufferTimeMs)
                .build();
        BatchSubmitter<DummyRequest> batchSubmitter = factory.getBatchSubmitter();
        List<Future> futures = new ArrayList<>();
        int numRecordsToSubmit = 100;
        for (int i = 0; i < numRecordsToSubmit; i++) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            batchSubmitter.put(new DummyRequest(i, future));
            futures.add(future);
        }
        futures.forEach(future -> {
            try {
                future.get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException("Future did not complete in time.  Got: " + mockWriter.getBatchesWritten());
            }
        });

        MatcherAssert.assertThat(mockWriter.getNumWriteInvocations().get(), Matchers.greaterThan(1));
        List<Integer> itemsWritten = mockWriter.getBatchesWritten().stream().flatMap(List::stream).collect(Collectors.toList());
        List<Integer> expectedElements =  IntStream.range(0, numRecordsToSubmit).boxed().collect(Collectors.toList());
        MatcherAssert.assertThat(itemsWritten, Matchers.containsInAnyOrder(expectedElements.toArray()));
    }

}
