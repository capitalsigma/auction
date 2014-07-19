package com.readers;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import com.containers.WaitFreeQueue;


enum RecordType {
	Add, Modify, Delete
}

abstract class MarketOrderCollection extends HashMap<Integer, Integer> {
	TreeSet<Integer> sortedKeys;
	// In case we want to do something special later
	public MarketOrderCollection() {
		// default size (can't define as a property)
		super(10);
	}

	int[][] topN(int topN) {
		// can't zip lists in Java
		assert topN <= size();

		int[][] ret = new int[topN][2];

		// for(Integer i = 0; i < topN; i++) {
		int i = 0;
		for (Integer key : sortedKeys){
			ret[i][0] = key;
			ret[i][1] = get(key);

			if(i == topN) {
				break;
			}
		}

		return ret;
	}

	@Override
	public Integer put(Integer key, Integer value) {
		Integer ret;
		boolean didAdd;

		if((ret = super.put(key, value)) == null) {
			didAdd = sortedKeys.add(key);
			assert didAdd;
		}

		return ret;
	}

}

class SellOrders extends MarketOrderCollection {
	// ascending sort for prices, following utils.py
	public SellOrders(){
		super();
		sortedKeys = new TreeSet<Integer>();
	}
}

class BuyOrders extends MarketOrderCollection {
	// descending sort for prices, following utils.py
	public BuyOrders(){
		super();
		sortedKeys = new TreeSet<Integer>(Collections.reverseOrder());
	}
}

// TODO: lookup table of orders (for Modify behavior)
// TODO: implement Add, Delete, Modify
public class ArcaParser extends AbstractParser implements Runnable {
	WaitFreeQueue<String> inQueue;
	// not sure yet what this needs to be
	WaitFreeQueue<DataPoint> outQueue;
	String[] tickers;

	// TODO: would it be faster to try to use eg an enum here?
	Map<String, Map<OrderType, MarketOrderCollection>> ordersNow;

	// Split CSVs on commas
	String INPUT_SPLIT = ",";

	// We stop caring after the tenth CSV value
	int IMPORTANT_SYMBOL_COUNT = 10;

	// like in Python
	int LEVELS = 10;

	static Map<String, RecordType> recordTypeLookup;

	public ArcaParser(String[] _tickers,
					  WaitFreeQueue<String> _inQueue,
					  WaitFreeQueue<DataPoint> _outQueue) {
		tickers = _tickers;
		inQueue = _inQueue;
		outQueue = _outQueue;

		ordersNow = new HashMap<String,
			Map<OrderType, MarketOrderCollection>>(tickers.length);


		// First we initialize with empty hashmaps
		for (String ticker : tickers) {
			Map<OrderType, MarketOrderCollection> toAdd =
				new HashMap<OrderType, MarketOrderCollection>();
			toAdd.put(OrderType.Buy, new BuyOrders());
			toAdd.put(OrderType.Sell, new SellOrders());
			ordersNow.put(ticker, toAdd);
		}

		// If we haven't yet initialized recordTypeLookup, set it up
		// so that we can get enums from symbols later

		recordTypeLookup = new HashMap<String, RecordType>(3);
		recordTypeLookup.put("A", RecordType.Add);
		recordTypeLookup.put("M", RecordType.Modify);
		recordTypeLookup.put("D", RecordType.Delete);

	}

	void processRecord(RecordType recType,
					   long seqNum,
					   OrderType ordType,
					   int qty,
					   String ticker,
					   int price,
					   int timeStamp) {
		assert ordersNow.containsKey(ticker) &&
			ordersNow.get(ticker).containsKey(ordType);

		MarketOrderCollection toUpdate = ordersNow.get(ticker).get(ordType);
		Integer oldQty;

		if((oldQty = toUpdate.get(price)) == null) {
			oldQty = 0;
		}

		// TODO: deal with the different record types correctly
		toUpdate.put(price, qty + oldQty);

		DataPoint toPush = new DataPoint(ticker,
										 toUpdate.topN(LEVELS),
										 ordType,
										 timeStamp,
										 seqNum);
		// spin until we successfully push
		while(!outQueue.enq(toPush)) { }
	}


	// type, seq, _, _, b/s, count, ticker, price, sec, ms, _, _, _
	// type is A(dd) | M(odify) | D(elete)
	// b/s is B(uy) | S(ell)
	// 0:type, 1:seq, _, _, 4:b/s, 5:count, 6:ticker, 7:price, 8:sec, 9:ms, _, _, _
	public void run() {
		String toParse;
		String[] asSplit;

		RecordType recType;
		long seqNum; 		// need 10 digits
		OrderType ordType;
		int qty; 		// need 9 digits
		String ticker;
		int price;
		int timeStamp; 			// 8 digits


		// Stop work when the Gzipper tells us to
		while (inQueue.acceptingOrders) {

			// Work if we got something from the queue, otherwise spin
			if ((toParse = inQueue.deq()) != null) {
				// Split into fields
				asSplit = toParse.split(INPUT_SPLIT, IMPORTANT_SYMBOL_COUNT);

				// and convert to the types we want.
				if((recType = recordTypeLookup.get(asSplit[0])) == null){
					// skip if it's not add, modify, delete
					continue;
				}

				seqNum = Long.parseLong(asSplit[1]);
				ordType = asSplit[4].equals("B") ? OrderType.Buy : OrderType.Sell;
				qty = Integer.parseInt(asSplit[5]);
				ticker = asSplit[6];
				// we do his trick from original to floating point error
				price = Integer.parseInt(asSplit[7] + "000000");
				timeStamp = Integer.parseInt(asSplit[8]) * 1000
					+ Integer.parseInt(asSplit[9]);

				processRecord(recType, seqNum, ordType, qty, ticker, price,
							  timeStamp);
			}

		}

	}

}
