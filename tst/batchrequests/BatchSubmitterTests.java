package batchrequests;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

public class BatchSubmitterTests {

    @Test(expected = IllegalArgumentException.class)
    public void put_whenNullQueue_thenExceptionThrown() {
        new BatchSubmitter<>(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void put_whenNoQueues_thenExceptionThrown() {
        new BatchSubmitter<>(new ArrayList<>());
    }

    @Test(expected = IllegalArgumentException.class)
    public void put_whenNonRandomAccessList_thenExceptionThrown() {
        LinkedList<QueueAndLock> listOfQueues = new LinkedList<>();
        listOfQueues.add(new QueueAndLock(new LinkedList(), new ReentrantLock()));
        new BatchSubmitter(listOfQueues);
    }

    @Test
    public void put_whenOneQueueAndMultiplePuts_thenSucceeds() {
        List<QueueAndLock<Integer>> queueAndLocks = Collections.singletonList(new QueueAndLock<>(new LinkedList<>(), new ReentrantLock()));
        BatchSubmitter<Integer> submitter = new BatchSubmitter<>(queueAndLocks);
        for (int i = 0; i < 5; i++) {
            submitter.put(i);
        }
        Assert.assertEquals(5, queueAndLocks.get(0).getQueue().size());
    }

    @Test
    public void put_whenMultipleQueueAndMultiplePuts_thenSucceeds() {
        QueueAndLock<Integer> queueAndLock1 = new QueueAndLock<>(new LinkedList<>(), new ReentrantLock());
        QueueAndLock<Integer> queueAndLock2 = new QueueAndLock<>(new LinkedList<>(), new ReentrantLock());
        List<QueueAndLock<Integer>> queueAndLocks = Arrays.asList(queueAndLock1, queueAndLock2);

        BatchSubmitter<Integer> submitter = new BatchSubmitter<>(queueAndLocks);
        for (int i = 0; i < 5; i++) {
            submitter.put(i);
        }
        Assert.assertEquals(3, queueAndLock1.getQueue().size());
        Assert.assertEquals(2, queueAndLock2.getQueue().size());
    }

    @RequiredArgsConstructor
    @Getter
    private class PutRunnable<T> implements Runnable {
        private final BatchSubmitter<T> batchSubmitter;
        private final T valueToSubmit;

        @Override
        public void run() {
            batchSubmitter.put(valueToSubmit);
        }
    }

    @Test
    public void put_multiThreaded_thenSucceeds() throws Exception {
        QueueAndLock<Integer> queueAndLock1 = new QueueAndLock<>(new LinkedList<>(), new ReentrantLock());
        QueueAndLock<Integer> queueAndLock2 = new QueueAndLock<>(new LinkedList<>(), new ReentrantLock());
        List<QueueAndLock<Integer>> queueAndLocks = Arrays.asList(queueAndLock1, queueAndLock2);
        BatchSubmitter<Integer> submitter = new BatchSubmitter<>(queueAndLocks);

        ExecutorService executorService = Executors.newFixedThreadPool(4);
        int numsSubmitted = 100;
        for (int i = 0; i < numsSubmitted; i++) {
            executorService.submit(new PutRunnable<>(submitter, i));
        }
        executorService.shutdown();
        Assert.assertEquals("Expected queues to be put in a reasonable amount of time",
                true, executorService.awaitTermination(5, TimeUnit.SECONDS));

        LinkedList<Integer> combinedList = new LinkedList<>();
        combinedList.addAll(queueAndLock1.getQueue());
        combinedList.addAll(queueAndLock2.getQueue());
        MatcherAssert.assertThat(combinedList, Matchers.containsInAnyOrder(IntStream.range(0, numsSubmitted).boxed().toArray()));
    }
}