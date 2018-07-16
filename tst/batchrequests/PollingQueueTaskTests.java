package batchrequests;

import org.hamcrest.Matchers;
import org.hamcrest.junit.MatcherAssert;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Integration tests so that we can block on completion of writing without waiting
 */
public class PollingQueueTaskTests {

    @Mock private BatchWriter<Integer, Void> mockWriter;
    private Queue<Integer> queueForMockWriter;
    private ReentrantLock lockForMockWriter;
    private static final int MAX_BATCH_SIZE = 5;
    private static final long BUFFER_TIME_MS = 1L;
    private PollingQueueTask<Integer> pollingQueueTask;
    private CountDownLatch waitForWriteLatch;

    @Captor ArgumentCaptor<Collection<Integer>> mockWriterPerformWriteCaptor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        queueForMockWriter = new LinkedList<>();
        lockForMockWriter = new ReentrantLock();
        waitForWriteLatch = new CountDownLatch(1);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                waitForWriteLatch.countDown();
                return null;
            }
        }).when(mockWriter).performWrite(mockWriterPerformWriteCaptor.capture());

        pollingQueueTask = new PollingQueueTask<>(queueForMockWriter, lockForMockWriter, mockWriter, MAX_BATCH_SIZE,
                BUFFER_TIME_MS);
    }

    @Test
    public void run_whenStartingWithFullBatch_thenWrites() throws Exception {
        // Start with a full batch so the worker immediately processes it
        for (int i = 0; i < MAX_BATCH_SIZE; i++) {
            queueForMockWriter.add(i);
        }

        // Start the task
        Thread thread = new Thread(pollingQueueTask);
        thread.start();

        // Wait for completion without timeout (It'll be obvious if there's something wrong)
        waitForWriteLatch.await();
        pollingQueueTask.shutdown();

        // The worker should never end without releasing all locks
        Assert.assertEquals(0, lockForMockWriter.getHoldCount());

        // There should have been at least one invocation to processWrites
        Mockito.verify(mockWriter, Mockito.atLeastOnce()).performWrite(Mockito.any());
        MatcherAssert.assertThat(mockWriterPerformWriteCaptor.getAllValues().size(), Matchers.greaterThanOrEqualTo(1));
        MatcherAssert.assertThat(mockWriterPerformWriteCaptor.getAllValues().get(0), Matchers.containsInRelativeOrder(0,1,2,3,4));
    }

    @Test
    public void run_whenStartingWithLessThanFullBatch_thenWrites() throws Exception {
        // Start with a full batch so the worker immediately processes it
        for (int i = 0; i < MAX_BATCH_SIZE - 1; i++) {
            queueForMockWriter.add(i);
        }

        // Start the task
        Thread thread = new Thread(pollingQueueTask);
        thread.start();

        // Wait for completion without timeout (It'll be obvious if there's something wrong)
        waitForWriteLatch.await();
        pollingQueueTask.shutdown();

        // The worker should never end without releasing all locks
        Assert.assertEquals(0, lockForMockWriter.getHoldCount());

        // There should have been at least one invocation to processWrites
        Mockito.verify(mockWriter, Mockito.atLeastOnce()).performWrite(Mockito.any());
        MatcherAssert.assertThat(mockWriterPerformWriteCaptor.getAllValues().size(), Matchers.greaterThanOrEqualTo(1));
        MatcherAssert.assertThat(mockWriterPerformWriteCaptor.getAllValues().get(0), Matchers.containsInRelativeOrder(0,1,2,3));
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