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
	String TEST_OUT = ResolvablePath.resolve("test/test-data/test-out.h5");
	// String TEST_OUT = "./test-data/test-out.h5";

	// @Test
	public void testInstantiate() throws Exception {
		HDF5Writer writer = null;
		try {
			// System.out.println("Trying to open: " + TEST_OUT);

			// for (FileFormat f : FileFormat.getFileFormats()) {
			// 	System.out.println("Found a new format: " + f.toString());
			// 	// System.out.println("Full name: " + f.fullFileName);
			// }

			// System.out.println("Supported ext: " + FileFormat.getFileExtensions());

			writer = new HDF5Writer(new WaitFreeQueue<DataPoint>(5),
									TEST_OUT);
		} catch (Throwable t) {
			fail("Exception thrown during instantiation: " +
				 t.toString());
		} finally {
			writer.closeFile();
		}

	}

	@Test
	public void testInitializeFile() throws Exception {
		HDF5Writer writer = null;
		try {
			writer = new HDF5Writer(new WaitFreeQueue<DataPoint>(5),
									TEST_OUT);

			writer.initializeFile();
		} finally {
			writer.closeFile();
		}
	}


}
