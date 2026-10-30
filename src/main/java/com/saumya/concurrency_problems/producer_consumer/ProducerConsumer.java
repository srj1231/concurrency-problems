package com.saumya.concurrency_problems.producer_consumer;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
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
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }
        this.maxCapacity = capacity;
        this.queue = new ArrayDeque<>(); // ArrayDeque has constant time O(1) for add and remove, low memory overhead
    }

    public void put(E item) throws InterruptedException {
        Objects.requireNonNull(item); // throws NullPointerException if item is null
        lock.lockInterruptibly(); // so thread doesn't wait indefinitely if another thread holds the lock
        // it throws InterruptedException if another thread interrupts it

        try {
            while(queue.size() == maxCapacity) {
                notFull.await(); // blocks the producer thread
                // problem: two producer threads may wake up and both try to add, leading to an overflow
                // solution: use a while loop instead of if
            }
            queue.add(item);
            notEmpty.signal();  // wakes up a waiting consumer thread
        } finally {
            lock.unlock();
        }
    }

    public boolean offer(E item, long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(item);

        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (queue.size() == maxCapacity) {
                if(nanos <= 0) return false; // exit if queue has no space after waiting until timeout
                nanos = notFull.awaitNanos(nanos); // update remaining time before giving up
            }
            queue.add(item);
            notEmpty.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }


    public E get() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while(queue.isEmpty()) {
                notEmpty.await();   // blocks the consumer thread
                // problem: two consumer threads may wake up and both try to poll, leading to a null value
                // solution: use a while loop instead of if
            }
            E item = queue.remove(); // remove throws NoSuchElementException if queue is empty
            notFull.signal(); // wakes up a waiting producer thread
            return item;
        } finally {
            lock.unlock();
        }
    }
    
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();

        try {
            while (queue.isEmpty()) {
                if (nanos <= 0) return null;    // If timeout expires, returns null.
                nanos = notEmpty.awaitNanos(nanos);
            }

            E item = queue.remove();
            notFull.signal();
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
                    if (!pc.offer(item, 500, TimeUnit.MILLISECONDS)) {
                        // null if timeout expires before item is added
                        System.out.println("Failed to produce: " + item + " (queue full)");
                    } else {
                        System.out.println("Produced: " + item);
                    }
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
                    Integer item = pc.poll(500, TimeUnit.MILLISECONDS);
                    if (item == null) { // null if timeout expires before item is available
                        System.out.println("No item available (timeout)");
                    } else {
                        System.out.println("Consumed: " + item);
                    }
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
