package com.saumya.concurrency_problems.producer_consumer;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantLock;

public class ProducerConsumer<E> {
    private final Queue<E> queue;
    private int maxCapacity = 16;
    private final ReentrantLock lock = new ReentrantLock();

    public ProducerConsumer(int capacity) {
        this.maxCapacity = capacity;
        this.queue = new LinkedList<>();
    }

    public void put(E item) {
        lock.lock();
        try {
            queue.add(item);
        } finally {
            lock.unlock();
        }

    }

    public E get() {
        lock.lock();
        try {
            return queue.poll();
        } finally {
            lock.unlock();
        }
    }
}
