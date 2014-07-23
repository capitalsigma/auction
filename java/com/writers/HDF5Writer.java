package com.writers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import ncsa.hdf.object.Datatype;
import ncsa.hdf.object.Attribute;
import ncsa.hdf.object.FileFormat;
import ncsa.hdf.hdf5lib.HDF5Constants;
import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;
import ncsa.hdf.object.Group;
import ncsa.hdf.object.h5.H5Datatype;
import ncsa.hdf.object.h5.H5File;

import com.containers.WaitFreeQueue;
import com.readers.DataPoint;

public class HDF5Writer implements Runnable {
	HashMap<String, Group> tickerGroups;
	H5File fileHandle;
	WaitFreeQueue<DataPoint> inQueue;
	Group rootGroup;

	// Here we try to open up a new HDF5 file in the path we've been
	// given, and raise abstraction-appropriate exceptions if
	// something goes wrong.
	public HDF5Writer(WaitFreeQueue<DataPoint> _inQueue,
					  String outPath)
		throws HDF5FormatNotFoundException, HDF5FileNotOpenedException {


	    FileFormat fileFormat = FileFormat.getFileFormat(FileFormat.FILE_TYPE_HDF5);

		if(fileFormat == null) {
			throw new HDF5FormatNotFoundException();
		}

		// TODO: By default, we overwrite an existing HDF5 file -- may
		// want to consider "-force" flag
		try {
			fileHandle = (H5File) fileFormat.createFile(outPath,
														FileFormat.FILE_CREATE_DELETE);
			// fileHandle = new H5File(outPath,
			// 						FileFormat.FILE_CREATE_DELETE);

			fileHandle.open();
		} catch (HDF5Exception e) {
			System.out.println("Caught: " + e.toString());
			System.out.println("Message: " + e.getMessage());
			throw new HDF5FileNotOpenedException();
		} catch (Throwable t) {
			System.out.println("Caught: " + t.toString());
			throw new HDF5FileNotOpenedException();
		}

		rootGroup =
			(Group) ((javax.swing.tree.DefaultMutableTreeNode)
					 fileHandle.getRootNode()).getUserObject();

		// TODO: we know how big it is, we should inst. appropriately
		tickerGroups = new HashMap<String, Group>();
		inQueue = _inQueue;

	}


	// FIXME: do something better than throw Exception
	Datatype newOrderType(String name) throws Exception {


		return null;
	}

	// define our datatypes
	// TODO: throw good exn
	public void initializeFile() throws HDF5Exception {
		// try {
		// String titleStr = "Equity Data";

		// Datatype titleType = createDatatypeForString(titleStr);
		// Attribute titleAttr = new Attribute("TITLE",
		// 									titleType,
		// 									new long[] {1},
		// 									new String[] {titleStr});

		Attribute titleAttr = createAttributeForString("TITLE",
													   "Equity Data");

		fileHandle.writeAttribute(rootGroup, titleAttr, false);



		// } catch (Throwable t) {

		// }

	}

	Datatype createDatatypeForString(String attrValue) {
		Datatype ret = new H5Datatype(Datatype.CLASS_STRING,
									  attrValue.length() + 1,
									  Datatype.ORDER_LE,
									  Datatype.SIGN_NONE);

		return ret;
	}

	Attribute createAttributeForString(String attrName, String attrValue) {
		Datatype attrType = createDatatypeForString(attrValue);
		Attribute attr = new Attribute(attrName,
									   attrType,
									   new long[] {1},
									   new String[] {attrValue});

		return attr;
	}

	public void closeFile() throws HDF5Exception {
		fileHandle.close();
	}


	int createBooksDt() throws HDF5LibraryException {
		// We can't use the nice OOP interface to build a compound
		// datatype that has arrays inside of it (b/c it's nested). So
		// instead, we use the ugly low-level one.
		int sizeofSeqNum = 8;
		int sizeofTimeStamp = 4;

		int orderArrColumns = 2;
		int orderArrRows = 10;
		int sizeofOrderCell = 8;
		int numOrderArrs = 2;

		int sizeofOrderArr = orderArrColumns * orderArrRows * sizeofOrderCell;
		int sizeofOrderArrs = numOrderArrs * sizeofOrderArr;

		int totalSize = sizeofSeqNum + sizeofTimeStamp + sizeofOrderArrs;


		int dtOrderArr = H5.H5Tarray_create(HDF5Constants.H5T_STD_I64LE,
											2,
											new long[] {orderArrRows,
														orderArrColumns});
		int dtSeqNum = HDF5Constants.H5T_STD_I64LE;
		int dtTimeStamp = HDF5Constants.H5T_STD_I32LE;

		int dtCompound = H5.H5Tcreate(HDF5Constants.H5T_COMPOUND,
									  totalSize);

		int result1 = H5.H5Tinsert(dtCompound,
								   "ask",
								   0,
								   dtOrderArr);
		assert result1 >= 0;

		int result2 = H5.H5Tinsert(dtCompound,
								   "bid",
								   sizeofOrderArr,
								   dtOrderArr);
		assert result2 >= 0;

		int result3 = H5.H5Tinsert(dtCompound,
									  "seqnum",
									  sizeofOrderArrs,
									  dtSeqNum);
		assert result3 >= 0;

		int result4 =  H5.H5Tinsert(dtCompound,
									"timestamp",
									sizeofOrderArrs + sizeofSeqNum,
									dtTimeStamp);

		assert result4 > 0;
	}

	void createBooksDs(Group toAdd) {
		// TODO: somehow we get toAdd's loc_id, then we use H5Acreate


	}

	public Group initializeGroup(String ticker) throws HDF5Exception {
		try {
			Group toRet = fileHandle.createGroup(ticker, rootGroup);
			Attribute classAttr = createAttributeForString("CLASS",
														   "Group");

			fileHandle.writeAttribute(toRet, classAttr, false);



			return toRet;
		} catch (Throwable t) {
			System.err.println("Caught exn: " + t.toString());
			throw new HDF5GroupException();
		}
	}



	// if we have that in our file already, return it. if not, make a new one
	Group getGroup(String ticker) throws HDF5GroupException {
		// Here, we keep track of our groups as we make them, because
		// there's no quick and easy way in the HDF5 library to find a
		// particular child of a group without looping through the
		// whole thing.
		Group toRet = tickerGroups.get(ticker);

		if(toRet == null) {
			try {
				// then we didn't have it yet
				Group toAdd = initializeGroup(ticker);
				tickerGroups.put(ticker, toAdd);
			} catch (Throwable t) {
				throw new HDF5GroupException();
			}
		}

		return toRet;
	}

	public void run() {
		DataPoint dataPoint;
		Group toAdd;

		try {
			while(inQueue.acceptingOrders || !inQueue.isEmpty()) {
				if((dataPoint = inQueue.deq()) != null) {
					toAdd = getGroup(dataPoint.ticker);
				}

			}
		} catch (HDF5Exception e) {
			System.err.println("An exception occurred: " + e.toString());
			System.err.println("Aborting.");
			return;

		} finally {
			try {
				// fileHandle.close();
				closeFile();
			} catch (Throwable t) {
				System.err.println("An exception occured while " +
								   "trying to save the current file: " +
								   t.toString());
				System.err.println("Failing to save.");
			}
		}
	}
}
