package batchrequests;

import java.util.Collection;

/**
 * The writer that will perform the batch write to the desired source.  To be implemented
 * by the client.
 * @param <T> Type of input to the batch
 */
public interface BatchWriter<T> {

    /**
     * Write the collected batch to the desired source.
     * Ensure that all exceptions (checked or unchecked) are caught and handled appropriately.
     * @param batchRequests The requests that have been batched, to be written by this method.
     */
    void write(Collection<T> batchRequests);
}
