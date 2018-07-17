package batchrequests;

import org.hamcrest.Matchers;
import org.hamcrest.junit.MatcherAssert;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Integration tests so that we can block on completion of writing without waiting
 */
public class PollingQueueTaskTests {

    @Mock private BatchWriter<Integer> mockWriter;
    private Queue<Integer> queueForMockWriter;
    private ReentrantLock lockForMockWriter;
    private static final int MAX_BATCH_SIZE = 5;
    private static final long BUFFER_TIME_MS = 1L;
    private PollingQueueTask<Integer> pollingQueueTask;
    private CountDownLatch waitForWriteLatch;

    @Captor private ArgumentCaptor<Collection<Integer>> mockWriterPerformWriteCaptor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        queueForMockWriter = new LinkedList<>();
        lockForMockWriter = new ReentrantLock();
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                waitForWriteLatch.countDown();
                return null;
            }
        }).when(mockWriter).write(mockWriterPerformWriteCaptor.capture());

        pollingQueueTask = new PollingQueueTask<>(queueForMockWriter, lockForMockWriter, mockWriter, MAX_BATCH_SIZE,
                BUFFER_TIME_MS);
    }

    @Test
    public void run_whenStartingWithFullBatch_thenWrites() throws Exception {
        waitForWriteLatch = new CountDownLatch(1);

        // Start with a full batch so the worker immediately processes it
        for (int i = 0; i < MAX_BATCH_SIZE; i++) {
            queueForMockWriter.add(i);
        }

        // Start the task
        Thread thread = new Thread(pollingQueueTask);
        thread.start();

        // Wait with long timeout that shouldn't be reached in any normal scenario (For error notification reasons)
        waitForWriteLatch.await(10, TimeUnit.SECONDS);
        pollingQueueTask.shutdown();

        // The worker should never end without releasing all locks
        Assert.assertEquals(0, lockForMockWriter.getHoldCount());

        // There should have been at least one invocation to processWrites
        Mockito.verify(mockWriter, Mockito.atLeastOnce()).write(Mockito.any());
        List<Collection<Integer>> capturedValues = mockWriterPerformWriteCaptor.getAllValues();
        MatcherAssert.assertThat(capturedValues.size(),Matchers.greaterThanOrEqualTo(1));
        MatcherAssert.assertThat(capturedValues.get(0), Matchers.containsInRelativeOrder(0,1,2,3,4));
    }

    @Test
    public void run_whenStartingWithLessThanFullBatch_thenWrites() throws Exception {
        waitForWriteLatch = new CountDownLatch(1);

        // Start with a full batch so the worker immediately processes it
        for (int i = 0; i < MAX_BATCH_SIZE - 1; i++) {
            queueForMockWriter.add(i);
        }

        // Start the task
        Thread thread = new Thread(pollingQueueTask);
        thread.start();

        // Wait with long timeout that shouldn't be reached in any normal scenario (For error notification reasons)
        waitForWriteLatch.await(10, TimeUnit.SECONDS);
        pollingQueueTask.shutdown();

        // The worker should never end without releasing all locks
        Assert.assertEquals(0, lockForMockWriter.getHoldCount());

        // There should have been at least one invocation to processWrites
        Mockito.verify(mockWriter, Mockito.atLeastOnce()).write(Mockito.any());
        List<Collection<Integer>> capturedValues = mockWriterPerformWriteCaptor.getAllValues();
        MatcherAssert.assertThat("Error in captured values: " + capturedValues.toString(),
                capturedValues.size(), Matchers.greaterThanOrEqualTo(1));
        MatcherAssert.assertThat("Error in captured values: " + capturedValues.toString(), capturedValues.get(0),
                Matchers.containsInRelativeOrder(0,1,2,3));
    }

    @Test
    public void run_whenSleepingAndMoreThanBatchSizeComesIn_thenOnlyBatchSizeIsWritten() throws Exception {
        waitForWriteLatch = new CountDownLatch(2);

        // Start with a full batch so the worker immediately processes it
        for (int i = 0; i < MAX_BATCH_SIZE + 3; i++) {
            queueForMockWriter.add(i);
        }

        // Start the task
        Thread thread = new Thread(pollingQueueTask);
        thread.start();

        // Wait with long timeout that shouldn't be reached in any normal scenario (For error notification reasons)
        waitForWriteLatch.await(10, TimeUnit.SECONDS);
        pollingQueueTask.shutdown();

        // The worker should never end without releasing all locks
        Assert.assertEquals(0, lockForMockWriter.getHoldCount());

        // There should have been at least one invocation to processWrites
        Mockito.verify(mockWriter, Mockito.atLeastOnce()).write(Mockito.any());
        List<Collection<Integer>> capturedValues = mockWriterPerformWriteCaptor.getAllValues();
        MatcherAssert.assertThat("Error in captured values: " + capturedValues.toString(),
                capturedValues.size(), Matchers.greaterThanOrEqualTo(2));
        MatcherAssert.assertThat("Error in captured values: " + capturedValues.toString(),
                capturedValues.get(0), Matchers.containsInRelativeOrder(0,1,2,3,4));
        MatcherAssert.assertThat("Error in captured values: " + capturedValues.toString(),
                capturedValues.get(1), Matchers.containsInRelativeOrder(5,6,7));
    }

    // TODO: This is not a reliable test because it waits
    @Test
    public void run_whenSleepInterrupted_thenNoFailure() throws Exception {
        waitForWriteLatch = new CountDownLatch(1);
        pollingQueueTask = new PollingQueueTask<>(queueForMockWriter, lockForMockWriter, mockWriter, MAX_BATCH_SIZE,
                10_000);

        // Start the task with an empty queue, to force a block
        Thread thread = new Thread(pollingQueueTask);
        thread.start();

        // Wait until processing has actually started to interrupt
        Thread.sleep(500);

        // Interrupt
        thread.interrupt();

        // Wait for a bit for the task to finish
        waitForWriteLatch.await(10, TimeUnit.SECONDS);
        Thread.sleep(500);

        // Shutdown flag should be est
        Assert.assertEquals(true, pollingQueueTask.isShutdown());
    }

    // TODO: This is not a reliable test because it waits
    @Test
    public void run_whenThreadInterrupted_thenNoFailure() throws Exception {
        int numThreads = 5;
        waitForWriteLatch = new CountDownLatch(5);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<PollingQueueTask> tasks = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            PollingQueueTask task = new PollingQueueTask<>(queueForMockWriter, lockForMockWriter, mockWriter, MAX_BATCH_SIZE,
                    10_000);
            executor.submit(task);
            tasks.add(task);
        }

        // Wait a bit for tasks to start
        Thread.sleep(500);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }
}