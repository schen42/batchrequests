package batchrequests;

import batchrequests.util.DummyBatchWriter;
import batchrequests.util.DummyRequest;
import org.hamcrest.Matchers;
import org.hamcrest.junit.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
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
        DummyBatchWriter mockWriter = DummyBatchWriter.getSucceedingDummyBatchWriter();
        ReentrantLock lock = new ReentrantLock();
        Queue<DummyRequest> queue = new LinkedList<>();
        PollingQueueTask<DummyRequest> pollingQueueTask =
                new PollingQueueTask<>(queue, lock, mockWriter, batchSize, bufferTimeMs);

        // Start with a full batch so the worker immediately processes it
        List<Future<Void>> futures = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            queue.add(new DummyRequest(i, future));
        }

        // Start the task
        Thread thread = new Thread(pollingQueueTask);
        thread.start();

        // Wait for completion
        futures.stream().forEach(future -> {
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

        // Finish
        pollingQueueTask.shutdown();
    }

    @Test
    public void run_whenStartingWithLessThanFullBatch_thenWrites() throws Exception {
        long bufferTimeMs = 1L;
        int batchSize = 5;
        DummyBatchWriter mockWriter = DummyBatchWriter.getSucceedingDummyBatchWriter();
        ReentrantLock lock = new ReentrantLock();
        Queue<DummyRequest> queue = new LinkedList<>();
        PollingQueueTask<DummyRequest> pollingQueueTask =
                new PollingQueueTask<>(queue, lock, mockWriter, batchSize, bufferTimeMs);

        // Start with a full batch so the worker immediately processes it
        List<Future<Void>> futures = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            queue.add(new DummyRequest(i, future));
        }

        // Start the task
        Thread thread = new Thread(pollingQueueTask);
        thread.start();

        // Wait for completion
        futures.stream().forEach(future -> {
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

        // Finish
        pollingQueueTask.shutdown();
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