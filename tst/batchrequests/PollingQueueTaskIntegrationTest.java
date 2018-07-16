package batchrequests;

import batchrequests.util.DummyBatchWriteResultProcessor;
import batchrequests.util.DummyBatchWriter;
import batchrequests.util.DummyRequest;
import org.hamcrest.Matchers;
import org.hamcrest.junit.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TODO make these tests not sleep
 */
public class PollingQueueTaskIntegrationTest {

    @Test
    public void run_whenStartingWithFullBatch_thenWrites() throws Exception {
        long bufferTimeMs = 1L;
        int batchSize = 5;
        DummyBatchWriteResultProcessor processor = new DummyBatchWriteResultProcessor();
        DummyBatchWriter mockWriter = new DummyBatchWriter(processor, false);
        ReentrantLock lock = new ReentrantLock();
        Queue<DummyRequest> queue = new LinkedList<>();
        PollingQueueTask<DummyRequest> pollingQueueTask =
                new PollingQueueTask<>(queue, lock, mockWriter, batchSize, bufferTimeMs);

        // Start with a full batch so the worker immediately processes it
        List<Future<Void>> futures = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            queue.add(new DummyRequest(i, future));
            futures.add(future);
        }

        // Start the task
        Thread thread = new Thread(pollingQueueTask);
        thread.start();

        // Wait for completion
        futures.forEach(future -> {
            try {
                future.get(bufferTimeMs * 10, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                throw new RuntimeException("Timed out waiting for batch to be written", e);
            }
        });

        // The worker should never end without release all locks
        Assert.assertEquals(0, lock.getHoldCount());

        // There should have been at least one invocation to processWrites
        MatcherAssert.assertThat(mockWriter.getNumWriteInvocations().get(), Matchers.greaterThan(0));
        Assert.assertEquals(1, processor.getResults().size());
        Assert.assertEquals(Arrays.asList(0,1,2,3,4), processor.getResults().get(0));

        // Finish
        pollingQueueTask.shutdown();
    }

    @Test
    public void run_whenStartingWithLessThanFullBatch_thenWrites() throws Exception {
        long bufferTimeMs = 1L;
        int batchSize = 5;
        DummyBatchWriteResultProcessor processor = new DummyBatchWriteResultProcessor();
        DummyBatchWriter mockWriter = new DummyBatchWriter(processor, false);
        ReentrantLock lock = new ReentrantLock();
        Queue<DummyRequest> queue = new LinkedList<>();
        PollingQueueTask<DummyRequest> pollingQueueTask =
                new PollingQueueTask<>(queue, lock, mockWriter, batchSize, bufferTimeMs);

        // Start with a full batch so the worker immediately processes it
        List<Future<Void>> futures = new ArrayList<>();
        for (int i = 0; i < batchSize - 1; i++) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            queue.add(new DummyRequest(i, future));
            futures.add(future);
        }

        // Start the task
        Thread thread = new Thread(pollingQueueTask);
        thread.start();

        // Wait for completion
        futures.forEach(future -> {
            try {
                future.get(bufferTimeMs * 10, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                throw new RuntimeException("Timed out waiting for batch to be written", e);
            }
        });

        // The worker should never end without release all locks
        Assert.assertEquals(0, lock.getHoldCount());

        // There should have been at least one invocation to processWrites
        MatcherAssert.assertThat(mockWriter.getNumWriteInvocations().get(), Matchers.greaterThan(0));
        Assert.assertEquals(1, processor.getResults().size());
        Assert.assertEquals(Arrays.asList(0,1,2,3), processor.getResults().get(0));

        // Finish
        pollingQueueTask.shutdown();
    }

    @Test
    public void run_whenSleepingAndMoreThanBatchSizeComesIn_thenOnlyBatchSizeIsWritten() {
        Assert.fail();
    }

    @Test
    public void run_whenSleepInterrupted_thenNoFailure() {
        Assert.fail();
    }

    @Test
    public void run_whenThreadInterrupted_thenNoFailure() {
        Assert.fail();
    }
}