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
            if(queue.size() == maxCapacity) {
                notFull.await(); // blocks the producer thread
            }
            queue.add(item);
        } finally {
            lock.unlock();
        }

    }

    public E get() throws InterruptedException {
        lock.lock();
        try {
            if(queue.isEmpty()) {
                notEmpty.await();   // blocks the consumer thread
            }
            return queue.poll();
        } finally {
            lock.unlock();
        }
    }
}
