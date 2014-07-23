package test;

import static org.junit.Assert.*;

import ncsa.hdf.object.FileFormat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.containers.WaitFreeQueue;
import com.readers.DataPoint;
import com.writers.HDF5Writer;
import com.writers.ResolvablePath;

@RunWith(JUnit4.class)
public class HDF5WriterTest {
	// String TEST_OUT = ResolvablePath.resolve("./test-data/test-out.h5");
	String TEST_OUT = "./test-data/test-out.h5";
	@Test
	public void testInstantiate() throws Exception {
		try {

			HDF5Writer writer = new HDF5Writer(new WaitFreeQueue<DataPoint>(5),
											   TEST_OUT);
		} catch (Throwable t) {
			fail("Exception thrown during instantiation: " +
				 t.toString());
		}

	}

}
