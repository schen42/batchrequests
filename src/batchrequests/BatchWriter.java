package batchrequests;

import java.util.Collection;

/**
 * TODO
 * @param <T> Type of input to the batch
 */
public interface BatchWriter<T> {

    void write(Collection<T> batchRequests);
}
