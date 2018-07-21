package batchrequests;

import org.hamcrest.Matchers;
import org.hamcrest.junit.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Integration tests so that we can block on completion of writing without waiting
 */
public class PollingQueueTaskTests {

    private static class TestScaffold {

        /** The mock writer used in the {@link PollingQueueTask} to perform mocks on */
        @Mock BatchWriter<Integer> mockWriter;

        /** Can use this captor to inspect what was written to {@link #mockWriter} */
        @Captor private ArgumentCaptor<Collection<Integer>> mockWriterPerformWriteCaptor;

        /** A queue to seed values that will be read by the task */
        Queue<Integer> queueForMockWriter;

        /** The lock used in the {@link PollingQueueTask} */
        ReentrantLock lockForMockWriter;

        /** The batch size to be used in the {@link PollingQueueTask} */
        static final int MAX_BATCH_SIZE = 5;

        /** The buffer time to be used in the {@link PollingQueueTask} */
        static final long BUFFER_TIME_MS = 1L;

        /**
         * A {@link PollingQueueTask} polling queue task that batches Integer requests setup with the parameters
         * provided by {@link TestScaffold}.
         */
        PollingQueueTask<Integer> pollingQueueTask;

        /**
         * A latch that will be used to wait for the first write to happen.  For example, it can be used for
         * waiting for the {@link BatchWriter#write(Collection)} to occur before capturing
         * the arguments used for the write.
         */
        CountDownLatch waitForWriteLatch;

        /**
         * A latch that will be used to wait for the second write to happen.  For example, it can be used by
         * the test to block further iterations of the {@link PollingQueueTask} from continuing.
         */
        CountDownLatch waitForNextWriteLatch;
    }


    /**
     * @return See {@link TestScaffold} on how to use it.
     */
    private TestScaffold setupTestWithWaitForTaskStart(int expectedNumberOfWrites) {
        TestScaffold scaffold = new TestScaffold();

        MockitoAnnotations.initMocks(scaffold);

        scaffold.queueForMockWriter = new LinkedList<>();
        scaffold.lockForMockWriter = new ReentrantLock();
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                scaffold.waitForWriteLatch.countDown();
                return null;
            }
        }).when(scaffold.mockWriter).write(scaffold.mockWriterPerformWriteCaptor.capture());
        scaffold.waitForWriteLatch = new CountDownLatch(expectedNumberOfWrites);
        scaffold.pollingQueueTask = new PollingQueueTask<>(
                scaffold.queueForMockWriter,
                scaffold.lockForMockWriter,
                scaffold.mockWriter,
                scaffold.MAX_BATCH_SIZE,
                scaffold.BUFFER_TIME_MS);
        return scaffold;
    }

    @Test
    public void run_whenStartingWithFullBatch_thenWrites() throws Exception {
        TestScaffold scaffold = setupTestWithWaitForTaskStart(1);

        // Start with a full batch so the worker immediately processes it
        for (int i = 0; i < scaffold.MAX_BATCH_SIZE; i++) {
            scaffold.queueForMockWriter.add(i);
        }

        // Start the task
        Thread thread = new Thread(scaffold.pollingQueueTask);
        thread.start();

        // Wait with long timeout that shouldn't be reached in any normal scenario (For error notification reasons)
        Assert.assertEquals("Expected write to occur within a reasonable amount of time",
                true, scaffold.waitForWriteLatch.await(10, TimeUnit.SECONDS));
        scaffold.pollingQueueTask.shutdown();

        // The worker should never end without releasing all locks
        Assert.assertEquals(0, scaffold.lockForMockWriter.getHoldCount());

        // There should have been at least one invocation to processWrites
        Mockito.verify(scaffold.mockWriter, Mockito.atLeastOnce()).write(Mockito.any());
        List<Collection<Integer>> capturedValues = scaffold.mockWriterPerformWriteCaptor.getAllValues();
        MatcherAssert.assertThat(capturedValues.size(),Matchers.greaterThanOrEqualTo(1));
        MatcherAssert.assertThat(capturedValues.get(0), Matchers.containsInRelativeOrder(0,1,2,3,4));
        Assert.assertEquals(true, scaffold.pollingQueueTask.isShutdown());
    }

    @Test
    public void run_whenStartingWithLessThanFullBatch_thenWrites() throws Exception {
        TestScaffold scaffold = setupTestWithWaitForTaskStart(1);

        // Start with a full batch so the worker immediately processes it
        for (int i = 0; i < scaffold.MAX_BATCH_SIZE - 1; i++) {
            scaffold.queueForMockWriter.add(i);
        }

        // Start the task
        Thread thread = new Thread(scaffold.pollingQueueTask);
        thread.start();

        // Wait with long timeout that shouldn't be reached in any normal scenario (For error notification reasons)
        scaffold.waitForWriteLatch.await(10, TimeUnit.SECONDS);
        scaffold.pollingQueueTask.shutdown();

        // The worker should never end without releasing all locks
        Assert.assertEquals(0, scaffold.lockForMockWriter.getHoldCount());

        // There should have been at least one invocation to processWrites
        Mockito.verify(scaffold.mockWriter, Mockito.atLeastOnce()).write(Mockito.any());
        List<Collection<Integer>> capturedValues = scaffold.mockWriterPerformWriteCaptor.getAllValues();
        MatcherAssert.assertThat("Error in captured values: " + capturedValues.toString(),
                capturedValues.size(), Matchers.greaterThanOrEqualTo(1));
        MatcherAssert.assertThat("Error in captured values: " + capturedValues.toString(), capturedValues.get(0),
                Matchers.containsInRelativeOrder(0,1,2,3));
        Assert.assertEquals(true, scaffold.pollingQueueTask.isShutdown());
    }

    @Test
    public void run_whenSleepingAndMoreThanBatchSizeComesIn_thenOnlyBatchSizeIsWritten() throws Exception {
        TestScaffold scaffold = setupTestWithWaitForTaskStart(2);

        // Start with a full batch so the worker immediately processes it
        for (int i = 0; i < scaffold.MAX_BATCH_SIZE + 3; i++) {
            scaffold.queueForMockWriter.add(i);
        }

        // Start the task
        Thread thread = new Thread(scaffold.pollingQueueTask);
        thread.start();

        // Wait with long timeout that shouldn't be reached in any normal scenario (For error notification reasons)
        Assert.assertEquals("Expected write to occur within a reasonable amount of time",
                true, scaffold.waitForWriteLatch.await(10, TimeUnit.SECONDS));
        scaffold.pollingQueueTask.shutdown();

        // The worker should never end without releasing all locks
        Assert.assertEquals(0, scaffold.lockForMockWriter.getHoldCount());

        // There should have been at least one invocation to processWrites
        Mockito.verify(scaffold.mockWriter, Mockito.atLeastOnce()).write(Mockito.any());
        List<Collection<Integer>> capturedValues = scaffold.mockWriterPerformWriteCaptor.getAllValues();
        MatcherAssert.assertThat("Error in captured values: " + capturedValues.toString(),
                capturedValues.size(), Matchers.greaterThanOrEqualTo(2));
        MatcherAssert.assertThat("Error in captured values: " + capturedValues.toString(),
                capturedValues.get(0), Matchers.containsInRelativeOrder(0,1,2,3,4));
        MatcherAssert.assertThat("Error in captured values: " + capturedValues.toString(),
                capturedValues.get(1), Matchers.containsInRelativeOrder(5,6,7));
        Assert.assertEquals(true, scaffold.pollingQueueTask.isShutdown());
    }

    @Test
    public void run_whenBatchWriterThrowsRuntimeException_thenRunnableContinues() throws Exception {
        TestScaffold scaffold = setupTestWithWaitForTaskStart(1);

        // Overwrite scaffold to throw Exception after counting down the latch the first time
        // and then wait for another latch the second time to verify the call happened twice
        CountDownLatch secondCallLatch = new CountDownLatch(1);
        Mockito.doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                scaffold.waitForWriteLatch.countDown();
                throw new RuntimeException();
            }
        }).doAnswer(new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                secondCallLatch.countDown();
                return null;
            }
        }).when(scaffold.mockWriter).write(scaffold.mockWriterPerformWriteCaptor.capture());

        // Start with a full batch so the worker immediately processes it
        for (int i = 0; i < scaffold.MAX_BATCH_SIZE; i++) {
            scaffold.queueForMockWriter.add(i);
        }

        // Start the task
        Thread thread = new Thread(scaffold.pollingQueueTask);
        thread.start();

        // Wait with long timeout that shouldn't be reached in any normal scenario (For error notification reasons)
        Assert.assertEquals("Expected write to occur within a reasonable amount of time",
                true, scaffold.waitForWriteLatch.await(10, TimeUnit.SECONDS));

        // Wait with long timeout that shouldn't be reached in any normal scenario (For error notification reasons)
        // If it is reached, the thread is probably dead
        Assert.assertEquals("Expected second write to occur within a reasonable amount of time",
                true, secondCallLatch.await(10, TimeUnit.SECONDS));

        scaffold.pollingQueueTask.shutdown();
    }
}