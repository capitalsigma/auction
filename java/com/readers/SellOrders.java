package com.readers;

import java.util.TreeSet;

public class SellOrders extends MarketOrderCollection {
	// ascending sort for prices, following utils.py
	public SellOrders(){
		super();
		sortedKeys = new TreeSet<Integer>();
	}
}
