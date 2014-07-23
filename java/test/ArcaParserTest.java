package test;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.readers.ArcaParser;
import com.readers.DataPoint;
import com.readers.OrderType;
import com.containers.WaitFreeQueue;

@RunWith(JUnit4.class)
public class ArcaParserTest {
	String[] TEST_TICKERS = {"FOO", "BAR"};

	String TEST_ADD_BUY1 =
		"A,1,12884901908,B,B,1000,FOO,2.75,28800,737,B,AARCA,";
	String TEST_ADD_BUY2 =
		"A,1,12884902050,B,B,3200,BAR,0.98,28800,737,B,AARCA,";

	String TEST_ADD_SELL1 =
		"A,8,12884902687,B,S,30000,FOO,0.02,28800,739,B,AARCA,";
	String TEST_ADD_SELL2 =
		"A,12,12884902091,B,S,200000,BAR,0.0195,28800,740,B,AARCA,";

	String TEST_DELETE_BUY1 =
		"D,2,12884901908,28800,857,FOO,B,B,AARCA,B,";


	@Test
	public void testInstantiate() {
		try {
			ArcaParser parser = new ArcaParser(TEST_TICKERS,
											   new WaitFreeQueue<String>(3),
											   new WaitFreeQueue<DataPoint>(3));
		} catch (Throwable t) {
			fail("Exception thrown during instantiation: " +
				 t.toString());
		}
	}

	// ArcaParser buildParser(int capacity) {
	// 	return new ArcaParser(TEST_TICKERS,
	// 						  new WaitFreeQueue<String>(capacity),
	// 						  new WaitFreeQueue<DataPoint>(capacity));
	// }

	@Test
	public void testAdd() throws Exception {
		WaitFreeQueue<String> inQ = new WaitFreeQueue<String>(5);
		WaitFreeQueue<DataPoint> outQ = new WaitFreeQueue<DataPoint>(5);

		ArcaParser parser = new ArcaParser(TEST_TICKERS, inQ, outQ);

		inQ.enq(TEST_ADD_BUY1);
		// inQ.enq(TEST_ADD_BUY2);
		inQ.enq(TEST_ADD_SELL1);
		// inQ.enq(TEST_ADD_SELL2);

		Thread runThread = new Thread(parser);
		// parser.run();
		runThread.start();


		// sleep 200ms
		Thread.sleep(200);


		int[][] expectedBuy = {{2750000, 1000}};
		int[][] expectedSell = {{20000, 30000}};

		DataPoint buy1Expected =
			new DataPoint("FOO", expectedBuy, OrderType.Buy, 28800737, 1);

		DataPoint sell1Expected =
			new DataPoint("FOO", expectedSell, OrderType.Sell, 28800739, 8);

		assertTrue(buy1Expected.equals(outQ.deq()));
		assertTrue(sell1Expected.equals(outQ.deq()));
	}

	@Test
	public void testDelete() throws Exception {
		WaitFreeQueue<String> inQ = new WaitFreeQueue<String>(5);
		WaitFreeQueue<DataPoint> outQ = new WaitFreeQueue<DataPoint>(5);

		ArcaParser parser = new ArcaParser(TEST_TICKERS, inQ, outQ);

		inQ.enq(TEST_ADD_BUY1);
		inQ.enq(TEST_ADD_BUY2);
		inQ.enq(TEST_DELETE_BUY1);

		Thread runThread = new Thread(parser);
		// parser.run();
		runThread.start();


		// sleep 200ms
		Thread.sleep(200);

		// throw away the first two, we don't care (add is tested elsewhere)
		outQ.deq();
		outQ.deq();


		int[][] expectedOrders = {};
		DataPoint expected = new DataPoint("FOO", expectedOrders, OrderType.Buy,
										   28800857, 2);


		assertTrue(expected.equals(outQ.deq()));
	}
}
