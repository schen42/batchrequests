package batchrequests;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Optional;

// TODO: This needs to be done in most convenient way for implementation (easiest way to split successful and unsuccessful calls)
// For DynamoDB, part of the batch fails
// Returning multiple items is more general
// More general, but takes more memory
// DynamoDB gives rows that fails, but CloudWatch does not...
@AllArgsConstructor
@Getter
public class BatchWriteResult<T, U> {
    private Optional<T> failedRequests; // T can be anything, a list or a single object
    private Optional<U> successfulResults; // U can be anything, a list or a single object
}
