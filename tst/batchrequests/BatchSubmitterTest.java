package batchrequests;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class BatchSubmitterTest {

    @Test(expected = IllegalArgumentException.class)
    public void put_whenNullQueue_thenExceptionThrown() {
        new BatchSubmitter<>(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void put_whenNoQueues_thenExceptionThrown() {
        new BatchSubmitter<>(new LinkedList<>());
    }

    @Test()
    public void put_whenOneQueueAndMultiplePuts_thenSucceeds() {
        Queue<String> queue = new LinkedList<>();
        List<Queue<String>> queues = Collections.singletonList(queue);
        BatchSubmitter<String> submitter = new BatchSubmitter<>(queues);
        for (int i = 0; i < 5; i++) {
            submitter.put(Integer.toString(i));
        }
        Assert.assertEquals(5, queue.size());
    }

    @Test()
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
}