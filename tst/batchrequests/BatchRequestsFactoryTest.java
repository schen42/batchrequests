package batchrequests;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class BatchRequestsFactoryTest {

    @Mock
    private BatchWriter mockWriter;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void test_builder() {
        BatchRequestsFactory factory = new BatchRequestsFactory.BatchRequestsFactoryBuilder(mockWriter)
                .withMaxBufferTimeMs(123)
                .withBatchSize(456)
                .withNumPollingWorkersPerQueue(7)
                .build();
        Assert.assertEquals(123, factory.getMaxBufferTimeMs());
        Assert.assertEquals(456, factory.getBatchSize());
        Assert.assertEquals(7, factory.getNumPollingWorkersPerQueue());
        Assert.assertNotEquals(null, factory.getBatchSubmitter());
        Assert.assertNotEquals(null, factory.getBatchWriter());
    }

    @Test
    public void test_builderWithQueuesOption() {
        List<QueueAndLock> queues = Collections.singletonList(new QueueAndLock(new LinkedList(), new ReentrantLock()));
        BatchRequestsFactory factory = new BatchRequestsFactory.BatchRequestsFactoryBuilder(mockWriter)
                .withQueues(queues)
                .build();
        Assert.assertEquals(queues, factory.getQueueAndLocks());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_builderWithQueuesOptionAndEmptyQueuesProvided_thenFailure() {
        new BatchRequestsFactory.BatchRequestsFactoryBuilder(mockWriter)
                .withQueues(new ArrayList<>())
                .build();
    }

    @Test
    public void test_builderWithNumQueuesOption() {
        BatchRequestsFactory factory = new BatchRequestsFactory.BatchRequestsFactoryBuilder(mockWriter)
                .withNumQueues(2)
                .build();
        Assert.assertEquals(2, factory.getQueueAndLocks().size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_builderWithNumQueuesOptionAndNonPositiveNum_thenFailure() {
        BatchRequestsFactory factory = new BatchRequestsFactory.BatchRequestsFactoryBuilder(mockWriter)
                .withNumQueues(0)
                .build();
        Assert.assertEquals(2, factory.getQueueAndLocks().size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_builderWithNumQueuesAndQueueOptions_thenFailure() {
        BatchRequestsFactory factory = new BatchRequestsFactory.BatchRequestsFactoryBuilder(mockWriter)
                .withNumQueues(2)
                .withQueues(new ArrayList(new LinkedList()))
                .build();
        Assert.assertEquals(2, factory.getQueueAndLocks().size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_constructorNullQueueList_thenFailure() {
        new BatchRequestsFactory(mockWriter, null, 1, 1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_constructorEmptyQueueList_thenFailure() {
        new BatchRequestsFactory(mockWriter, new ArrayList<>(), 1, 1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_constructorNonReadOnlyList_thenFailure() {
        new BatchRequestsFactory(mockWriter, new LinkedList<>(), 1, 1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_constructorInvalidBatchSize_thenFailure() {
        new BatchRequestsFactory(mockWriter, new ArrayList(Collections.singletonList(new LinkedList())), 0, 1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_constructorInvalidNumPollingWorkers_thenFailure() {
        new BatchRequestsFactory(mockWriter, new ArrayList(Collections.singletonList(new LinkedList())), 1, 0, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_constructorInvalidBufferTime_thenFailure() {
        new BatchRequestsFactory(mockWriter, new ArrayList(Collections.singletonList(new LinkedList())), 1, 1, 0);
    }
}