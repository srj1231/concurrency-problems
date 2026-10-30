package com.saumya.concurrency_problems.producer_consumer;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

public class ProducerConsumerWithWaitAndNotify<E> {
    private final Queue<E> queue;
    private final int maxCapacity;
    private static final AtomicInteger counter = new AtomicInteger(1);

    public ProducerConsumerWithWaitAndNotify(int capacity) {
        this.maxCapacity = capacity;
        this.queue = new LinkedList<>();
    }

    public synchronized void put(E item) throws InterruptedException {
        while(queue.size() == maxCapacity) {
            this.wait();
        }
        queue.add(item);
        this.notifyAll();
    }

    public synchronized E get() throws InterruptedException {
        while(queue.isEmpty()) {
            this.wait();
        }
        E item = queue.poll();
        this.notifyAll();
        return item;
    }

    public static void main(String[] args) throws InterruptedException {
        ProducerConsumerWithWaitAndNotify<Integer> pc = new ProducerConsumerWithWaitAndNotify<>(3);

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
