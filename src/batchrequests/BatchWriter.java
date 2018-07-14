package batchrequests;

import java.util.Collection;
import java.util.Optional;

/**
 * TODO
 * @param <T> Type of input to the batch
 * @param <U> Type of result from the batch call
 */
public abstract class BatchWriter<T, U> {

    private final Optional<BatchWriteResultProcessor<U>> processor;

    public BatchWriter() {
       this(null);
    }

    public BatchWriter(BatchWriteResultProcessor<U> processor) {
        this.processor = Optional.ofNullable(processor);
    }

    abstract public U write(Collection<T> batch);

    public void performWrite(Collection<T> batch) {
        try {
            U batchWriteResult = write(batch);
            processor.ifPresent(processor -> processor.processResult(batchWriteResult));
        } catch (Exception e) {
            processor.ifPresent(processor -> processor.handleException(e));
        }
    }
}
