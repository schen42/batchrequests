package batchrequests.util;

import batchrequests.BatchWriter;
import lombok.Getter;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * TODO: Documentation
 */
@Getter
public class DummyBatchWriter implements BatchWriter<DummyRequest> {

    private final boolean shouldAlwaysFail;
    private final List<List<Integer>> batchesWritten;
    private AtomicInteger numWriteInvocations = new AtomicInteger(0);

    public DummyBatchWriter(boolean shouldAlwaysFail) {
        this.shouldAlwaysFail = shouldAlwaysFail;
        this.batchesWritten = new LinkedList<>();
    }

    @Override
    public void write(Collection<DummyRequest> batch) {
        numWriteInvocations.getAndIncrement();
        if (shouldAlwaysFail) {
            throw new RuntimeException("shouldAlwaysFail is set to true");
        }
        synchronized (batchesWritten) {
            batchesWritten.add(batch.stream().map(DummyRequest::getRequestValue).collect(Collectors.toList()));
        }
        batch.stream().forEach(request -> request.getCompletableFuture().complete(null));
    }
}
