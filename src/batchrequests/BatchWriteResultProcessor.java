package batchrequests;

import java.util.Collection;

/**
 * TODO
 * @param <T>
 * @param <U>
 */
public interface BatchWriteResultProcessor<T, U> {

    void processResult(Collection<T> request, U result);

    void handleException(Exception e);

}
