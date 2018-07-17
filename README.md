This package provides an interface for batching requests asynchronously.  It will allow you to make single requests
to simplify code and improve readability, but provide the benefits of making batch requests.

This package is intended to be used with long-running network calls that benefit from being batched.  For example:
* APIs that charge per request rather than by batch size.  If an API charges you the same price for one request as
a batch of 64 requests, you can reduce your cost by 64x.
* APIs that charge for data transfer with each request rounded up (e.g. AWS).  If you pay per 64KB, each request is 1KB,
and you are charged the same for 64KB because they're in separate requests, you can reduce your cost by 64x by batching
all requests into one.

# How To Use
Implement

# How To Build
This package uses Maven, so use your favorite way of building Maven projects.

# Tenets
* Be as easy to use as possible
* Be as lightweight as possible (as few dependencies as possible)

# To-dos
# TODO: Maybe move it to an issue tracker
* Javadocs writing
* License
* Put on Github
* Put on Maven
* Javadocs generation
* Performance profiling
* Put coverage badge on Github
* Optional retry mechanism (that can be handled via BatchSubmitter right now)
