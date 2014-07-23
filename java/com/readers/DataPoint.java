package com.readers;

import java.util.Arrays;

// maybe this should go in with the writers
public class DataPoint {
	public int[][] orders;
	public OrderType type;
	public int timeStamp;
	public long seqNum;
	public String ticker;

	public DataPoint(String _ticker,
					 int[][] _orders,
					 OrderType _type,
					 int _timeStamp,
					 long _seqNum) {
		ticker = _ticker;
		orders = _orders;
		type = _type;
		timeStamp = _timeStamp;
		seqNum = _seqNum;
	}


	public boolean equals(DataPoint other) {
		if(other == null) {
			return false;
		}

		boolean tickEq = other.ticker.equals(ticker);
		boolean seqEq = other.seqNum == seqNum;
		boolean tsEq = other.timeStamp == timeStamp;
		boolean otEq = other.type == type;
		boolean ordsEq = Arrays.deepEquals(other.orders, orders);

		System.out.println("TickEq? " + tickEq);
		System.out.println("SeqEq? " + seqEq);
		System.out.println("tsEq? " + tsEq);
		System.out.println("otEq? " + otEq);
		System.out.println("ordsEq? " + ordsEq);

		System.out.printf("my tick: %s, other tick: %s\n", ticker, other.ticker);
		System.out.printf("my seq: %d, other seq: %d\n", seqNum, other.seqNum);
		System.out.printf("my ts: %d, other ts: %d\n", timeStamp, other.timeStamp);
		System.out.printf("my ot: %d, other ot: %d\n",
						  type == OrderType.Buy ? 1 : 0,
						  other.type == OrderType.Buy ? 1 : 0);

		// System.out.println("my orders[0]: " + Arrays.toString(orders[0]));
		// System.out.println("other orders[0]: " + Arrays.toString(other.orders[0]));
		System.out.printf("my orders.length: %d, other orders.length: %d\n",
						  orders.length, other.orders.length);

		return other.ticker.equals(ticker) &&
			(other.seqNum == seqNum) &&
			(other.timeStamp == timeStamp) &&
			(other.type == type) &&
			Arrays.deepEquals(other.orders, orders);
	}
}
