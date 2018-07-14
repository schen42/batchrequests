package batchrequests;

/**
 * TODO
 * @param <U>
 */
public interface BatchWriteResultProcessor<U> {

    void processResult(U result);

    void handleException(Exception e);

}
