package com.containers;

//This is a parallel, wait-free queue that *only* works for a single
// producer, single consumer. It is fast, but IT WILL BREAK IF YOU PUT
// MORE THAN ONE THREAD ON EITHER SIDE. For more info, look up
// "Lamport Queue."

// Source: The Art of Multiprocessor Programming, p. 69
public class WaitFreeQueue<T> {
	volatile int head = 0, tail = 0;
	T[] items;
	volatile public boolean acceptingOrders;

	public WaitFreeQueue(int capacity) {
		items = (T[])new Object[capacity];
		head = 0;
		tail = 0;
		acceptingOrders = true;
	}

	public boolean enq(T x) {
		if(tail - head == items.length) {
			// throw new FullException();
			return false;
		} else {
			items[tail % items.length] = x;
			tail++;
			return true;
		}
	}

	public T deq() {
		if (isEmpty()) {
			// throw new EmptyException();
			return null;
		} else {
			T x = items[head % items.length];
			head++;
			return x;
		}
	}

	public boolean isEmpty() {
		return tail - head == 0;
	}

}
