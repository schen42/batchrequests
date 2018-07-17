package batchrequests;

import java.util.Collection;

/**
 * TODO
 * @param <T> Type of input to the batch
 */
public abstract class BatchWriter<T> {

    abstract public void performWrite(Collection<T> batchRequests);
}
