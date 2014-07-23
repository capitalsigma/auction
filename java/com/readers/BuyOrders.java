package com.readers;

import java.util.Collections;
import java.util.TreeSet;

public class BuyOrders extends MarketOrderCollection {
	// descending sort for prices, following utils.py
	public BuyOrders(int startCapacity){
		super(startCapacity);
		sortedKeys = new TreeSet<Integer>(Collections.reverseOrder());
	}

	public BuyOrders() {
		this(10);
	}
}
