package com.saumya.concurrency_problems.producer_consumer;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ProducerConsumer<E> {
    private final Queue<E> queue;
    private final int maxCapacity;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();
    private static final AtomicInteger counter = new AtomicInteger(1);

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
            notEmpty.signalAll();
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
            E item = queue.poll();
            notFull.signalAll();
            return item;
        } finally {
            lock.unlock();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ProducerConsumer<Integer> pc = new ProducerConsumer<>(3);

        final Runnable producer = () -> {
            while (true) {
                try {
                    int item = counter.getAndIncrement();
                    System.out.println("Produced: " + item);
                    pc.put(item);
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        };

        new Thread(producer).start();
        new Thread(producer).start();

        final Runnable consumer = () -> {
            while (true) {
                try {
                    Integer item = pc.get();
                    System.out.println("Consumed: " + item);
                    Thread.sleep(150);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        };

        new Thread(consumer).start();
        new Thread(consumer).start();

        Thread.sleep(1000);
        System.exit(0);
    }
}
