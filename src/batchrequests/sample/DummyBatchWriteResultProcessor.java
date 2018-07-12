package batchrequests.sample;

import batchrequests.BatchWriteResultProcessor;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

@AllArgsConstructor
@Getter
public class DummyBatchWriteResultProcessor implements BatchWriteResultProcessor<Integer, String> {

    private final AtomicInteger numSuccesses;
    private final AtomicInteger numFailuresWithoutException;
    private final AtomicInteger numFailuresWithException;

    @Override
    public void onSuccess(String successfulResponse) {
        numSuccesses.addAndGet(1);
    }

    @Override
    public void onFailure(Integer failedRequest, Optional<Exception> e) {
        /*if (callback.isPresent()) {
            numFailuresWithoutException
        } else {
            numFailuresWithoutException.addAndGet(1);
        }*/
    }
}
