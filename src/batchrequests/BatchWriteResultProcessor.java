package batchrequests;

import java.util.Optional;

// TODO:
// - (Breaking change) Add a return value, if possible
public interface BatchWriteResultProcessor<T, U> {

    void onSuccess(U successfulResponse);

    void onFailure(T failedRequest, Optional<Exception> e);

}
