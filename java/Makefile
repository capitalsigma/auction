TEST_DIR=test/
TEST_BIN=test-bin/
ADD_LIBS=-Djava.library.path=./dependencies/HDF_Group/HDFView/2.10.1/lib
OPTS=-ea ${ADD_LIBS}
RUN_TEST=java ${OPTS} org.junit.runner.JUnitCore


.PHONY: test

# compile-test: test/*.java
# 	javac test/*.java
# 	mv test/*.class test-bin/

compile:
	javac

test: #compile-test
	# ${RUN_TEST} test.GzipReaderTest
	# ${RUN_TEST} test.WaitFreeQueueTest
	# ${RUN_TEST} test.MarketOrderCollectionTest
	# ${RUN_TEST} test.ArcaParserTest
	${RUN_TEST} test.HDF5WriterTest
