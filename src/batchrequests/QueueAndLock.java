package batchrequests;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Queue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A class that represents a queue and a lock to synchronize access to that queue.
 *
 * Alternatively, we can use {@link java.util.concurrent.ConcurrentLinkedQueue}.  However, the current implementation
 * optimizes for less batch calls, and does so by calling the {@link Queue#size()} method which is described in the
 * Javadocs as "typically not very useful in concurrent applications" for ConcurrentLinkedQueue.
 * If/when performance matters, this class
 * should not be used, and the lock-free implementation of ConcurrentLinkedQueue should be used instead.
 *
 * @param <T> The type of object that the queue will hold
 */
@RequiredArgsConstructor
@Getter
public class QueueAndLock<T> {
    private final Queue<T> queue;
    private final ReentrantLock lock;
}
