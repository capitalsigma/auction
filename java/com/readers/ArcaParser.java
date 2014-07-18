package com.readers;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import com.containers.WaitFreeQueue;

enum OrderType {
	Buy, Sell
}

enum RecordType {
	Add, Modify, Delete
}

class MarketOrderCollection extends HashMap<Float, Float> {
	// In case we want to do something special later
	public MarketOrderCollection() {
		// default size (can't define as a property)
		super(10);
	}
}

public class ArcaParser extends AbstractParser implements Runnable {
	WaitFreeQueue<String> inQueue;
	// not sure yet what this needs to be
	WaitFreeQueue<Object> outQueue;
	String[] tickers;

	// TODO: would it be faster to try to use eg an enum here?
	Map<String, Map<OrderType, MarketOrderCollection>> ordersNow;

	String INPUT_SPLIT = ",";
	// We stop caring after the tenth CSV value
	int IMPORTANT_SYMBOL_COUNT = 10;

	static Map<String, RecordType> recordTypeLookup;

	public ArcaParser(String[] _tickers,
					  WaitFreeQueue<String> _inQueue,
					  WaitFreeQueue<Object> _outQueue) {
		tickers = _tickers;
		inQueue = _inQueue;
		outQueue = _outQueue;

		ordersNow = new HashMap<String,
			Map<OrderType, MarketOrderCollection>>(tickers.length);


		// First we initialize with empty hashmaps
		for (String ticker : tickers) {
			Map<OrderType, MarketOrderCollection> toAdd =
				new HashMap<OrderType, MarketOrderCollection>();
			toAdd.put(OrderType.Buy, new MarketOrderCollection());
			toAdd.put(OrderType.Sell, new MarketOrderCollection());
			ordersNow.put(ticker, toAdd);
		}

		// If we haven't yet initialized recordTypeLookup, set it up
		// so that we can get enums from symbols later

		recordTypeLookup = new HashMap<String, RecordType>(3);
		recordTypeLookup.put("A", RecordType.Add);
		recordTypeLookup.put("M", RecordType.Modify);
		recordTypeLookup.put("D", RecordType.Delete);

	}

	void updateOrdersNow(RecordType recType,
						 BigInteger seqNum,
						 OrderType ordType,
						 BigInteger qty,
						 String ticker,
						 double price,
						 long sec,
						 int ms) {
		// apply recent changes to ordersNow
	}

	void pushCurrentRecord() {
		// push what we have now on to the queue
	}

	// type, seq, _, _, b/s, count, ticker, price, sec, ms, _, _, _
	// type is A(dd) | M(odify) | D(elete)
	// b/s is B(uy) | S(ell)
	// 0:type, 1:seq, _, _, 4:b/s, 5:count, 6:ticker, 7:price, 8:sec, 9:ms, _, _, _
	public void run() {
		String toParse;
		String[] asSplit;
		int lastMs = 0;


		RecordType recType;
		BigInteger seqNum; 		// need 10 bytes
		OrderType ordType;
		BigInteger qty; 		// need 9 bytes
		String ticker;
		double price;
		long sec;				// need 5 bytes
		int ms = 0;				// need 3 bytes


		// Stop work when the Gzipper tells us to
		while (inQueue.acceptingOrders) {
			if (ms != lastMs) {
				pushCurrentRecord();
				ms = lastMs;
			}

			// Work if we got something from the queue, otherwise spin
			if ((toParse = inQueue.deq()) != null) {
				// Split into fields
				asSplit = toParse.split(INPUT_SPLIT, IMPORTANT_SYMBOL_COUNT);

				// and convert to the types we want
				recType = recordTypeLookup.get(asSplit[0]);
				seqNum = new BigInteger(asSplit[1]);
				ordType = asSplit[4].equals("B") ? OrderType.Buy : OrderType.Sell;
				qty = new BigInteger(asSplit[5]);
				ticker = asSplit[6];
				price = Double.valueOf(asSplit[7]);
				sec = Long.parseLong(asSplit[8], 10);
				ms = Integer.parseInt(asSplit[9]);

				updateOrdersNow(recType, seqNum, ordType, qty, ticker, price, sec, ms);
			}

		}

	}

}
