package batchrequests.util;

import batchrequests.BatchWriter;
import lombok.Getter;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * TODO: Documentation
 */
@Getter
public class DummyBatchWriter extends BatchWriter<DummyRequest, List<Integer>> {

    private final boolean shouldAlwaysFail;
    private AtomicInteger numWriteInvocations = new AtomicInteger(0);

    public DummyBatchWriter(DummyBatchWriteResultProcessor resultProcessor, boolean shouldAlwaysFail) {
        super(resultProcessor);
        this.shouldAlwaysFail = shouldAlwaysFail;
    }

    @Override
    public List<Integer> write(Collection<DummyRequest> batch) {
        numWriteInvocations.getAndIncrement();
        if (shouldAlwaysFail) {
            throw new RuntimeException("shouldAlwaysFail is set to true");
        }
        // Generate the value to return
        return batch.stream().map(DummyRequest::getRequestValue).collect(Collectors.toList());
    }
}
