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
        LinkedList listOfQueues = new LinkedList();
        listOfQueues.add(new LinkedList<>());
        new BatchSubmitter<>(listOfQueues);
    }

    @Test
    public void put_whenOneQueueAndMultiplePuts_thenSucceeds() {
        Queue<String> queue = new LinkedList<>();
        List<Queue<String>> queues = Collections.singletonList(queue);
        BatchSubmitter<String> submitter = new BatchSubmitter<>(queues);
        for (int i = 0; i < 5; i++) {
            submitter.put(Integer.toString(i));
        }
        Assert.assertEquals(5, queue.size());
    }

    @Test
    public void put_whenMultipleQueueAndMultiplePuts_thenSucceeds() {
        Queue<String> queue1 = new LinkedList<>();
        Queue<String> queue2 = new LinkedList<>();
        List<Queue<String>> queues = Arrays.asList(queue1, queue2);

        BatchSubmitter<String> submitter = new BatchSubmitter<>(queues);
        for (int i = 0; i < 5; i++) {
            submitter.put(Integer.toString(i));
        }
        Assert.assertEquals(3, queue1.size());
        Assert.assertEquals(2, queue2.size());
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
        Queue<Integer> queue1 = new LinkedList<>();
        Queue<Integer> queue2 = new LinkedList<>();
        List<Queue<Integer>> queues = Arrays.asList(queue1, queue2);
        BatchSubmitter<Integer> submitter = new BatchSubmitter<>(queues);

        ExecutorService executorService = Executors.newFixedThreadPool(4);
        int numsSubmitted = 100;
        for (int i = 0; i < numsSubmitted; i++) {
            executorService.submit(new PutRunnable<>(submitter, i));
        }
        executorService.shutdown();
        Assert.assertEquals("Expected queues to be put in a reasonable amount of time",
                true, executorService.awaitTermination(5, TimeUnit.SECONDS));

        LinkedList<Integer> combinedList = new LinkedList<>();
        combinedList.addAll(queue1);
        combinedList.addAll(queue2);
        MatcherAssert.assertThat(combinedList, Matchers.containsInAnyOrder(IntStream.range(0, numsSubmitted).boxed().toArray()));
    }
}