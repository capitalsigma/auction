package com.readers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;


public abstract class MarketOrderCollection extends HashMap<Integer, Integer> {
	TreeSet<Integer> sortedKeys;

	// In case we want to do something special later
	public MarketOrderCollection() {
		// default size (can't define as a property)
		this(10);
	}

	public MarketOrderCollection(int startCapacity) {
		super(startCapacity);
	}

	public int[][] topN(int topN) {
		// can't zip lists in Java

		int[][] ret = new int[Math.min(sortedKeys.size(), topN)][2];

		// for(Integer i = 0; i < topN; i++) {
		int i = 0;
		for (Integer price : sortedKeys){

			ret[i][0] = price;
			ret[i][1] = get(price);

			// System.out.println("Set for price: " + price + " with qty " +
			// 				   ret[i][1]);

			if(++i == topN) {
				break;
			}
		}

		return ret;
	}

	@Override
	public Integer put(Integer price, Integer quantity) {
		Integer ret;

		ret = super.put(price, quantity);

		// System.out.printf("Setting for price: %d, qty: %d\n", price, quantity);
		// System.out.println("About to update sortedKeys: " +
		// 				   Arrays.toString(sortedKeys.toArray()));


		if(quantity == 0) {
 			// System.out.println("Removing.");
			sortedKeys.remove(price);
		} else {
			// System.out.println("Adding.");
			sortedKeys.add(price);
		}

		// System.out.println("Updated sortedKeys: " +
		// 				   Arrays.toString(sortedKeys.toArray()));


		return ret;
	}

}
