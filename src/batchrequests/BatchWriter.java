package batchrequests;

import java.util.Collection;

/**
 * The writer that will perform the batch write to the desired source.  To be implemented
 * by the client and injected in the {@link BatchRequestsFactory}.
 * @param <T> Type of the request that will be batched.
 */
public interface BatchWriter<T> {

    /**
     * Write the collected batch to the desired source.
     * Ensure that all exceptions (checked or unchecked) are caught and handled appropriately.
     * @param batchRequests The requests that have been batched, to be written by this method.
     */
    void write(Collection<T> batchRequests);
}
