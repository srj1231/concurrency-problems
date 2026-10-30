package com.saumya.concurrency_problems.producer_consumer;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ProducerConsumer<E> {
    private final Queue<E> queue;
    private int maxCapacity = 16;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();

    public ProducerConsumer(int capacity) {
        this.maxCapacity = capacity;
        this.queue = new LinkedList<>();
    }

    public void put(E item) throws InterruptedException {
        lock.lock();
        try {
            while(queue.size() == maxCapacity) {
                notFull.await(); // blocks the producer thread
                // problem: two producer threads may wake up and both try to add, leading to an overflow
                // solution: use a while loop instead of if
            }
            queue.add(item);
        } finally {
            lock.unlock();
        }

    }

    public E get() throws InterruptedException {
        lock.lock();
        try {
            while(queue.isEmpty()) {
                notEmpty.await();   // blocks the consumer thread
                // problem: two consumer threads may wake up and both try to poll, leading to a null value
                // solution: use a while loop instead of if
            }
            return queue.poll();
        } finally {
            lock.unlock();
        }
    }
}
