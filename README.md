[![Build Status](https://travis-ci.org/schen42/batchrequests.svg?branch=master)](https://travis-ci.org/schen42/batchrequests)
[![Coverage Status](https://coveralls.io/repos/github/schen42/batchrequests/badge.svg)](https://coveralls.io/github/schen42/batchrequests)

This package provides an interface for batching requests asynchronously.  It will allow you to make single requests
to simplify code and improve readability, but provide the benefits of making batch requests.

For example, instead of structuring your code to collect batches of requests:

    BatchRequest batchRequest = ...;
    for (...) {
        Request request = new Request();
        if (numCollectedRequests == batchSize) {
            batchRequest.call();
            batchRequest = new BatchRequest();
            numCollectedRequests = 0;
        } else {
            batchRequest.add(request);
        }
    }
    batchRequest.call();

You can have your code structured at a request level and submit a request for batching:

    Request request = new Request();
    batchSubmitter.put(request);

This package is intended to be used with long-running network calls that benefit from being batched.  For example:
* APIs that charge per request rather than by batch size.  If an API charges you the same price for one request as
a batch of 64 requests, you can reduce your cost by 64x.
* APIs that charge for data transfer with each request rounded up (e.g. AWS).  If you pay per 64KB, each request is 1KB,
and you are charged the same for 64KB because they're in separate requests, you can reduce your cost by 64x by batching
all requests into one.

This package is currently optimized to reduce the number of batch calls, possibly at the expense of performance.
If performance is important, this package *may* not be for you (run performance tests to see if it is suitable for your needs).

# How To Use
Implement a `BatchWriter` and use it to construct `BatchRequestsFactory` with the desired batch settings.
Then, retrieve a `BatchSubmitter` to send requests to by calling the `BatchRequestsFactory#getBatchSubmitter` method.

# How To Build
This package uses Maven, so use your favorite way of building Maven projects.

Note that this project uses [Lombok](https://projectlombok.org/), which may require additional setup if you use an
IDE.

# Tenets
* Be as easy to use as possible
* Be as lightweight as possible (as few dependencies as possible)

# To-dos
TODO: Maybe move it to an issue tracker
* Javadocs writing
* Put on Maven
* Javadocs generation
* Performance profiling
* Put coverage badge on Github
* Optional retry mechanism (that can be handled via BatchSubmitter right now)
* Lock-free implementation

# Credits
* See the `pom.xml` for all dependencies used.
* [Jetbrains](https://www.jetbrains.com/) for IntelliJ CE, on which most of the project was developed.

