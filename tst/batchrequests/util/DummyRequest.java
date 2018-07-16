package batchrequests.util;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor
@Getter
public class DummyRequest {
    private final Integer requestValue;
    private final CompletableFuture<Void> completableFuture;

    @Override
    public String toString() {
        return Integer.toString(requestValue);
    }
}
