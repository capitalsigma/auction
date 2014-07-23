package com.readers;


import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.containers.WaitFreeQueue;

enum RecordType {
	Add, Modify, Delete
}

class Order {
	int price;
	int quantity;

	public Order(int _price, int _quantity) {
		price = _price;
		quantity = _quantity;
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

	// TODO: according to stackoverflow.com/questions/81346/, we can
	// save time if we roll a MutableLong to use for qtys. we can do
	// this later if it's not fast enough
	Map<String, Map<OrderType, MarketOrderCollection>> ordersNow;

	// We're here abusing the fact that (at least within a day), there
	// are less than 2^64 orders, and saving just the end of the
	// 20-digit order reference number. This may break.
	Map<Long, Order> orderHistory;

	// Split CSVs on commas
	String INPUT_SPLIT = ",";

	// We stop caring after the tenth CSV value
	int IMPORTANT_SYMBOL_COUNT = 10;

	// like in Python -- this is our top N values in buy/sell prices we care about
	int LEVELS = 10;

	// offset for integer part of the price -- we pass it around as a
	// float to avoid floating point error. So we make the int part
	// bigger by this amount, and add in the float part in the
	// lower-order digits.
	int PRICE_OFFSET_EXP = 6;
	int PRICE_INTEGER_OFFSET = (int)Math.pow(10, 6);

	// bump this up when in production
	int INITIAL_ODRDER_HISTORY_SIZE = 2000;

	static Map<String, RecordType> recordTypeLookup;

	public ArcaParser(String[] _tickers,
					  WaitFreeQueue<String> _inQueue,
					  WaitFreeQueue<DataPoint> _outQueue) {
		tickers = _tickers;
		inQueue = _inQueue;
		outQueue = _outQueue;

		ordersNow = new HashMap<String,
			Map<OrderType, MarketOrderCollection>>(tickers.length);

		orderHistory = new HashMap<Long, Order>(INITIAL_ODRDER_HISTORY_SIZE);


		// First we initialize with empty hashmaps
		for (String ticker : tickers) {
			Map<OrderType, MarketOrderCollection> toAdd =
				new HashMap<OrderType, MarketOrderCollection>();
			toAdd.put(OrderType.Buy, new BuyOrders());
			toAdd.put(OrderType.Sell, new SellOrders());
			ordersNow.put(ticker, toAdd);
		}

		// Set up a lookup table for our recordTypes
		recordTypeLookup = new HashMap<String, RecordType>(4);
		recordTypeLookup.put("A", RecordType.Add);
		recordTypeLookup.put("M", RecordType.Modify);
		recordTypeLookup.put("D", RecordType.Delete);

	}

	void processAdd(int qty,
					int price,
					MarketOrderCollection toUpdate) {
		Integer oldQty;

		if((oldQty = toUpdate.get(price)) == null) {
			oldQty = 0;
		}

		toUpdate.put(price, qty + oldQty);
	}

	void processDelete(long refNum,
					   MarketOrderCollection toUpdate) {
		assert orderHistory.containsKey(refNum);

		Order toDelete = orderHistory.get(refNum);
		int currentQty = toUpdate.get(toDelete.price);

		toUpdate.put(toDelete.price, currentQty - toDelete.quantity);

	}

	void processModify(long refNum,
					   int qty,
					   int price,
					   MarketOrderCollection toUpdate) {
		// Following the Python, we treat a modify as a delete
		// followed by an add. There may be a way to do this that
		// involves fewer read/write operations, but since everything
		// we're doing is O(1) it shouldn't be a big deal.
		assert orderHistory.containsKey(refNum);

		Order toModify = orderHistory.get(refNum);
		Integer currentQtyOfOldPrice = toUpdate.get(toModify.price);
		Integer currentQtyOfNewPrice = toUpdate.get(price);

		int qtyOfNewPriceToAdd = currentQtyOfNewPrice == null ?
			0 : currentQtyOfNewPrice;

		toUpdate.put(toModify.price, currentQtyOfOldPrice - toModify.quantity);
		toUpdate.put(price, qty + qtyOfNewPriceToAdd);

	}

	// default args, for delete's call
	// TODO: we can do this more cleanly
	void processRecord(RecordType recType,
					   long seqNum,
					   long refNum,
					   OrderType ordType,
					   String ticker,
					   int timeStamp) {
		processRecord(recType, seqNum, refNum, ordType, -1, ticker, -1, timeStamp);
	}

	void processRecord(RecordType recType,
					   long seqNum,
					   long refNum,
					   OrderType ordType,
					   int qty,
					   String ticker,
					   int price,
					   int timeStamp) {
		assert ordersNow.containsKey(ticker) &&
			ordersNow.get(ticker).containsKey(ordType);

		MarketOrderCollection toUpdate = ordersNow.get(ticker).get(ordType);


		switch(recType) {
		case Add:
			processAdd(qty, price, toUpdate);
			break;
		case Delete:
			processDelete(refNum, toUpdate);
			break;
		case Modify:
			processModify(refNum, qty, price, toUpdate);
			break;
		}

		DataPoint toPush = new DataPoint(ticker,
										 toUpdate.topN(LEVELS),
										 ordType,
										 timeStamp,
										 seqNum);

		System.out.println("About to push a DataPoint.");
		toPush.equals(toPush);
		// spin until we successfully push
		while(!outQueue.enq(toPush)) { }
	}


	int parsePrice(String priceString) {
		String[] parts = priceString.split("\\.");

		// System.out.println("got price: " + priceString);
		// System.out.println("parsing price: " + Arrays.toString(parts));

		assert parts.length == 2 && parts[1].length() <= 6;


		int intPart = Integer.parseInt(parts[0]);
		int floatPart = Integer.parseInt(parts[1]);
		int toRet = intPart * PRICE_INTEGER_OFFSET +
			(int)Math.pow(10, PRICE_OFFSET_EXP - parts[1].length()) * floatPart;

		// System.out.printf("int part: %d, float part: %d\n", intPart, floatPart);
		// System.out.printf("toRet: %d\n", toRet);



		return toRet;
	}

	int makeTimestamp(String seconds, String ms) {
		return Integer.parseInt(seconds) * 1000
			+ Integer.parseInt(ms);
	}

	long makeRefNum(String refNumStr) {
		int charsToTake = Math.min(19, refNumStr.length());
		return Long.parseLong(refNumStr.substring(0, charsToTake));
	}

	OrderType makeOrderType(String toParse) {
		return toParse.equals("B") ? OrderType.Buy : OrderType.Sell;
	}


	// type, seq, refNum, _, b/s, count, ticker, price, sec, ms, _, _, _
	// type is A(dd) | M(odify) | D(elete)
	// b/s is B(uy) | S(ell)
	// 0:type, 1:seq, 2:refNum, _, 4:b/s, 5:count, 6:ticker, 7:price, 8:sec,
	// 9:ms, _, _, _
	public void parseAdd(String[] asSplit) {
		String ticker;
		if(ordersNow.containsKey(ticker = asSplit[6])) {
			long seqNum; 		// need 10 digits
			long refNum;		// need 20, but just taking last 19
			OrderType ordType;
			int qty; 		// need 9 digits
			int price;
			int timeStamp; 			// 8 digits
			RecordType recType = RecordType.Add;

			seqNum = Long.parseLong(asSplit[1]);
			// drop last char out of 20

			refNum = makeRefNum(asSplit[2]);
			ordType = makeOrderType(asSplit[4]);
			qty = Integer.parseInt(asSplit[5]);
			// we do his trick from original to floating point error
			// price = // Integer.parseInt(asSplit[7] + "000000");
			price = parsePrice(asSplit[7]);

			timeStamp = makeTimestamp(asSplit[8], asSplit[9]);


			orderHistory.put(refNum, new Order(price, qty));

			processRecord(recType, seqNum, refNum, ordType,
						  qty, ticker, price, timeStamp);
		}
	}

	// 1:seq, 2:order id, 3:seconds, 4:ms, 9:type
	public void parseDelete(String[] asSplit) {
		String ticker;
		if(ordersNow.containsKey(ticker = asSplit[5])) {
			System.out.println("Parsing delete");
			long seqNum; 		// need 10 digits
			long refNum;		// need 20, but just taking last 19
			OrderType ordType;
			int timeStamp; 			// 8 digits
			RecordType recType = RecordType.Delete;

			seqNum = Long.parseLong(asSplit[1]);
			refNum = makeRefNum(asSplit[2]);
			timeStamp = makeTimestamp(asSplit[3], asSplit[4]);
			ordType = makeOrderType(asSplit[9]);

			processRecord(recType, seqNum, refNum, ordType, ticker, timeStamp);

			orderHistory.remove(refNum);
		}
	}

	public void parseModify(String[] asSplit) {

	}

	public void run() {
		String toParse;
		String[] asSplit;

		RecordType recType;


		// Stop work when the Gzipper tells us to, once we've pulled
		// everything out of his queue
		while (inQueue.acceptingOrders || !inQueue.isEmpty()) {

			// Work if we got something from the queue, otherwise spin
			if ((toParse = inQueue.deq()) != null) {
				// Split into fields
				asSplit = toParse.split(INPUT_SPLIT, IMPORTANT_SYMBOL_COUNT + 1);

				System.out.println("asSplit: " + Arrays.toString(asSplit));

				// Also note that containsKey is O(1)
				if((recType = recordTypeLookup.get(asSplit[0])) == null){
					// skip if it's not add, modify, delete
					continue;
				}

				switch(recType) {
				case Add:
					parseAdd(asSplit);
					break;
				case Modify:
					parseModify(asSplit);
					break;
				case Delete:
					parseDelete(asSplit);
					break;
				}
			}

		}

	}

}
