package test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.containers.WaitFreeQueue;
import com.readers.GzipReader;


@RunWith(JUnit4.class)
public class GzipReaderTest {
	String TEST_FILE_ONE = "one-line.txt.gz";
	String TEST_FILE_TWO = "four-lines.txt.gz";

	String TEST_ONE_TEXT = "This is test data.";
	String[] TEST_TWO_TEXTS = {
		"This is more",
		 "and more",
		 "and more",
		 "test data."
	};

	String RESOURCES_DIR = "/test/test-data/";

	String buildPath(String fileName){
		String root = System.getProperty("user.dir");
		String fullPath = root + RESOURCES_DIR + fileName;

		System.out.println("Full path: " + fullPath);

		return fullPath;
	}


	GzipReader instantiate(String fileName, WaitFreeQueue<String> wfq)
		throws IOException, FileNotFoundException {
		// ClassLoader loader = GzipReaderTest.class.getClassLoader();
		// String location = loader.getResource(pathToFile).toString();


		return new GzipReader(new FileInputStream(buildPath(fileName)),
							  wfq);
	}

	@Test
	public void testInstantiate() {
		try {
			GzipReader reader = instantiate(TEST_FILE_ONE,
											new WaitFreeQueue<String>(5));
		} catch (Throwable t) {
			Assert.fail("Exception thrown trying to instantiate: " + t.toString());
		}
	}

	void validateFirstOutput(WaitFreeQueue<String> wfq) {
		Assert.assertEquals(TEST_ONE_TEXT, wfq.deq());
	}

	void validateSecondOutput(WaitFreeQueue<String> wfq) {
		for (int i = 0; i < 4; i++) {
			Assert.assertEquals(TEST_TWO_TEXTS[i], wfq.deq());
		}
		Assert.assertNull(wfq.deq());
	}

	@Test
	public void testRun() {
		try {
			WaitFreeQueue<String> wfqOne = new WaitFreeQueue<String>(5);
			WaitFreeQueue<String> wfqTwo = new WaitFreeQueue<String>(5);

			GzipReader readerOne = instantiate(TEST_FILE_ONE, wfqOne);
			GzipReader readerTwo = instantiate(TEST_FILE_TWO, wfqTwo);


			readerOne.run();
			readerTwo.run();

			validateFirstOutput(wfqOne);
			validateSecondOutput(wfqTwo);

		} catch (IOException exn) {
			Assert.fail("Bad input file. Exception: " + exn.toString());
		}
	}
}
