package com.saumya.concurrency_problems.producer_consumer;

import java.util.LinkedList;
import java.util.Queue;

public class ProducerConsumer<E> {
    private Queue<E> queue;
    private int maxCapacity = 16;

    public ProducerConsumer(int capacity) {
        this.maxCapacity = capacity;
        this.queue = new LinkedList<>();
    }

    public void put(E item) {
        queue.add(item);
    }

    public E get() {
        return queue.poll();
    }
}
