package test;

import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.runners.statements.Fail;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.containers.WaitFreeQueue;


@RunWith(JUnit4.class)
public class WaitFreeQueueTest {
	@Test
	public void testInstantiate() {
		try {
			WaitFreeQueue<String> stringQueue = new WaitFreeQueue<String>(10);
			WaitFreeQueue<Integer> intQueue = new WaitFreeQueue<Integer>(15);
		} catch (Throwable t) {
			Assert.fail("Got exception thrown: " + t.toString());
		}
	}

	@Test
	public void testEnqAndDeq() {
		WaitFreeQueue<String> stringQueue = new WaitFreeQueue<String>(3);
		WaitFreeQueue<Integer> intQueue = new WaitFreeQueue<Integer>(3);

		for(int i = 0; i < 3; i++) {
			Assert.assertTrue(stringQueue.enq("Hello"));
			Assert.assertTrue(intQueue.enq(5));
		}

		Assert.assertFalse(stringQueue.enq("World"));
		Assert.assertFalse(intQueue.enq(6));

		for(int i = 0; i < 3; i++) {
			Assert.assertEquals("Hello", stringQueue.deq());
			Assert.assertEquals(Integer.valueOf(5), intQueue.deq());
		}

		Assert.assertNull(stringQueue.deq());
		Assert.assertNull(intQueue.deq());

	}

	@Test
	public void testAcceptingOrders() {
		WaitFreeQueue<String> stringQueue = new WaitFreeQueue<String>(3);

		Assert.assertTrue(stringQueue.acceptingOrders);
		stringQueue.acceptingOrders = false;
		Assert.assertFalse(stringQueue.acceptingOrders);
	}


}
