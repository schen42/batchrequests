package batchrequests.util;

import batchrequests.BatchWriteResultProcessor;
import lombok.Getter;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Documentation
 */
@Getter
public class DummyBatchWriteResultProcessor implements BatchWriteResultProcessor<DummyRequest, List<Integer>> {
    private final List<List<Integer>> results;

    public DummyBatchWriteResultProcessor() {
        results = new LinkedList<>();
    }

    @Override
    public void processResult(Collection<DummyRequest> requests, List<Integer> result) {
        System.out.println("processResult: " + result);
        synchronized (results) {
            if (!result.isEmpty()) {
                results.add(result);
                System.out.println("new results: " + results);
            }
        }
        // Also complete the futures
        requests.forEach(request -> request.getCompletableFuture().complete(null));
    }

    @Override
    public void handleException(Exception e) {
    }
}
