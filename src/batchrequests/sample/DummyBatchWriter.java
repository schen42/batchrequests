package batchrequests.sample;

import batchrequests.BatchWriter;
import batchrequests.BatchWriteResultProcessor;
import lombok.Getter;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Getter
public class DummyBatchWriter extends BatchWriter<Integer, String> {

    private final boolean shouldAlwaysFail;

    public DummyBatchWriter(Optional<BatchWriteResultProcessor<Integer, String>> callback, boolean shouldAlwaysFail) {
        super(callback);
        this.shouldAlwaysFail = shouldAlwaysFail;
    }

    @Override
    public void write(List<Integer> batch) {
        try {
            if (shouldAlwaysFail) {
                throw new RuntimeException("shouldAlwaysFail is set to true");
            }
            String result = batch.stream().map(x -> x.toString()).collect(Collectors.joining(",", "[", "]"));
        } catch (Exception e) {
            //if (callback.isPresent()) {
            //  callback.get().onFailure();
            //}
        }
    }
}
