package com.saumya.concurrency_problems.producer_consumer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Producer-Consumer pattern implementation using BlockingQueue.
 * Demonstrates thread-safe communication between producer and consumer threads
 * without explicit locks.
 */
public class ProducerConsumerWithBlockingQueue {
    private static final AtomicInteger counter = new AtomicInteger(1);

    public static void main(String[] args) throws InterruptedException {
        // Create a bounded blocking queue with capacity 3
        BlockingQueue<Integer> queue = new ArrayBlockingQueue<>(3);

        // Producer task: continuously adds items to the queue
        final Runnable producer = () -> {
            while (true) {
                try {
                    // put() blocks if queue is full, waiting for space
                    queue.put(createItem());
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        };

        new Thread(producer).start();
        new Thread(producer).start();

        // Consumer task: continuously removes items from the queue
        final Runnable consumer = () -> {
            while (true) {
                try {
                    // take() blocks if queue is empty, waiting for items
                    Integer item = queue.take();
                    consumeItem(item);
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
        
        System.exit(0); // Terminate the JVM since threads run in infinite loops
    }

    /**
     * Generates the next item in the sequence.
     * @return the next sequential integer
     */
    private static int createItem() {
        int item = counter.getAndIncrement();
        System.out.println("Produced: " + item);
        return item;
    }

    /**
     * Simulates item consumption by printing it to console.
     * @param item the item to consume
     */
    private static void consumeItem(Integer item) {
        System.out.println("Consumed: " + item);
    }
}
