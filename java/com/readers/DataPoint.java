package com.readers;

import java.util.Arrays;

// maybe this should go in with the writers
public class DataPoint {
	public int[][][] orders;
	public OrderType type;
	public int timeStamp;
	public long seqNum;
	public String ticker;

	public DataPoint(String _ticker,
					 int[][][] _orders,
					 OrderType _type,
					 int _timeStamp,
					 long _seqNum) {
		ticker = _ticker;
		orders = _orders;
		type = _type;
		timeStamp = _timeStamp;
		seqNum = _seqNum;
	}

	public void print() {
		System.out.printf("my tick: %s\n", ticker);
		System.out.printf("my seq: %d\n", seqNum);
		System.out.printf("my ts: %d\n", timeStamp);
		System.out.printf("my type: %s\n",
						  type == OrderType.Buy ? "Buy" : "Sell");

		System.out.printf("my arrays: %s\n", Arrays.deepToString(orders));
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

		System.out.println("This datapoint: ");
		this.print();
		System.out.println("Comparing to: ");
		other.print();


		return other.ticker.equals(ticker) &&
			(other.seqNum == seqNum) &&
			(other.timeStamp == timeStamp) &&
			(other.type == type) &&
			Arrays.deepEquals(other.orders, orders);
	}
}
