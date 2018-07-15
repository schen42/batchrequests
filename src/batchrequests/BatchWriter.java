package batchrequests;

import java.util.Collection;
import java.util.Optional;

/**
 * TODO
 * @param <T> Type of input to the batch
 * @param <U> Type of result from the batch call
 */
public abstract class BatchWriter<T, U> {

    private final Optional<BatchWriteResultProcessor<T, U>> processor;

    public BatchWriter() {
       this(null);
    }

    public BatchWriter(BatchWriteResultProcessor<T, U> processor) {
        this.processor = Optional.ofNullable(processor);
    }

    abstract public U write(Collection<T> batchRequests);

    public void performWrite(Collection<T> batchRequests) {
        try {
            U batchWriteResult = write(batchRequests);
            processor.ifPresent(processor -> processor.processResult(batchRequests, batchWriteResult));
        } catch (Exception e) {
            processor.ifPresent(processor -> processor.handleException(e));
        }
    }
}
