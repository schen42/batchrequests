package batchrequests;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.mockito.Matchers.any;

public class PollingQueueWorkerTests {

    @Mock
    private Queue mockQueue;

    private ReentrantLock lockForMockQueue;

    private QueueAndLock mockQueueAndLock;

    @Mock
    private BatchWriter mockWriter = Mockito.mock(BatchWriter.class);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        lockForMockQueue = new ReentrantLock();
        mockQueueAndLock = new QueueAndLock(mockQueue, lockForMockQueue);
    }

    @Test
    public void test_constructionIsSuccessful() throws Exception {
        PollingQueueWorker worker = new PollingQueueWorker<>(mockQueueAndLock, mockWriter, 1, 1, 10);
        Assert.assertEquals(true, worker.shutdown(1000));
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_whenNonPositiveBatchSize_thenFailure() {
        new PollingQueueWorker<>(mockQueueAndLock, mockWriter, 0, 1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_whenNonPositiveNumPollingThreads_thenFailure() {
        new PollingQueueWorker<>(mockQueueAndLock, mockWriter, 1, 0, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_whenNonPositiveBufferTime_thenFailure() {
        new PollingQueueWorker<>(mockQueueAndLock, mockWriter, 1, 1, 0);
    }

    @Test
    public void test_shutdownInvokedDuringProcessing_thenSuccessful() throws Exception {
        // Setup wait tasks to start
        int numThreads = 1;
        CountDownLatch latch = new CountDownLatch(numThreads);
        Mockito.doAnswer(invocationOnMock -> {
            latch.countDown();
            return null;
        }).when(mockWriter).write(any());

        // Start worker
        PollingQueueWorker worker = new PollingQueueWorker(mockQueueAndLock, mockWriter, 1, numThreads, 100);

        // Wait for task to start
        Assert.assertEquals("Expected for task to start within reasonable amount of time",
                true, latch.await(10, TimeUnit.SECONDS));

        // Make sure shutdown was successful
        Assert.assertEquals(true, worker.shutdown(10_000));
    }

    @Test
    public void test_whenConstructed_sameNumberOfFuturesAsNumPollingThreads() {
        PollingQueueWorker worker = new PollingQueueWorker<>(mockQueueAndLock, mockWriter, 1, 5, 100);
        Assert.assertEquals(5, worker.getTaskFutures().size());
    }

    @Test
    public void test_PollingQueueWorkTestBuilder_setSuccessfully() {

        PollingQueueWorker worker = new PollingQueueWorker.PollingQueueWorkerBuilder(mockQueueAndLock, mockWriter, 123)
                .setMaxBufferTime(456)
                .setNumPollingThreads(7)
                .build();

        Assert.assertEquals(mockQueueAndLock, worker.getQueueAndLock());
        Assert.assertEquals(mockWriter, worker.getBatchWriter());
        Assert.assertEquals(123, worker.getBatchSize());
        Assert.assertEquals(456, worker.getMaxBufferTimeMs());
        Assert.assertEquals(7, worker.getNumPollingThreads());
    }
}