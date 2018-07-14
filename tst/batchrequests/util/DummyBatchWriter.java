package batchrequests.util;

import batchrequests.BatchWriteResultProcessor;
import batchrequests.BatchWriter;
import lombok.Getter;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * TODO: Documentation
 */
@Getter
public class DummyBatchWriter extends BatchWriter<DummyRequest, String> {

    private final boolean shouldAlwaysFail;
    private AtomicInteger numWriteInvocations = new AtomicInteger(0);

    public DummyBatchWriter(BatchWriteResultProcessor<String> resultProcessor, boolean shouldAlwaysFail) {
        super(resultProcessor);
        this.shouldAlwaysFail = shouldAlwaysFail;
    }

    @Override
    public String write(Collection<DummyRequest> batch) {
        numWriteInvocations.getAndIncrement();
        if (shouldAlwaysFail) {
            throw new RuntimeException("shouldAlwaysFail is set to true");
        }
        // Generate the value to return
        String toReturn = batch.stream()
                .map(x -> x.getRequestValue().toString())
                .collect(Collectors.joining(",", "[", "]"));
        // Complete the future
        batch.stream().forEach(x -> x.getCompletableFuture().complete(null));
        return toReturn;
    }

    public static DummyBatchWriter getSucceedingDummyBatchWriter() {
        return new DummyBatchWriter(new DummyBatchWriteResultProcessor(), false);
    }
}
