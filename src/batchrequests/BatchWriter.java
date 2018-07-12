package batchrequests;

import java.util.List;
import java.util.Optional;

public abstract class BatchWriter<T, U> {

    private final Optional<BatchWriteResultProcessor<T, U>> callback;

    public BatchWriter(Optional<BatchWriteResultProcessor<T, U>> callback) {
        this.callback = callback;
    }

    abstract public void write(List<T> batch);
}
