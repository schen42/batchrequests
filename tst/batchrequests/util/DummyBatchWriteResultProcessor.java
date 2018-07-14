package batchrequests.util;

import batchrequests.BatchWriteResultProcessor;
import lombok.Getter;

import java.util.LinkedList;
import java.util.List;

/**
 * Documentation
 */
@Getter
public class DummyBatchWriteResultProcessor implements BatchWriteResultProcessor<String> {
    private List<String> results;

    public DummyBatchWriteResultProcessor() {
        results = new LinkedList<>();
    }

    @Override
    public void processResult(String result) {
        synchronized (results) {
            if (!result.isEmpty()) {
                results.add(result);
            }
        }
    }

    @Override
    public void handleException(Exception e) {
        synchronized (results) {
            results.add("Exception");
        }
    }
}
