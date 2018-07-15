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
public class DummyBatchWriteResultProcessor implements BatchWriteResultProcessor<DummyRequest, String> {
    private List<String> results;

    public DummyBatchWriteResultProcessor() {
        results = new LinkedList<>();
    }

    @Override
    public void processResult(Collection<DummyRequest> requests, String result) {
        synchronized (results) {
            if (!result.isEmpty()) {
                results.add(result);
            }
        }
        requests.stream().forEach(request -> request.getCompletableFuture().complete(null));
    }

    @Override
    public void handleException(Exception e) {
        synchronized (results) {
            results.add("Exception");
        }
    }
}
