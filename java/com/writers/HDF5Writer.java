package com.writers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import ncsa.hdf.object.Datatype;
import ncsa.hdf.object.Attribute;
import ncsa.hdf.object.FileFormat;
import ncsa.hdf.hdf5lib.HDF5Constants;
import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;
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


		// for (FileFormat f : FileFormat.getFileFormats()) {
		// 	System.out.println("Found a new format: " + f.toString());

		// }

		if(fileFormat == null) {
			throw new HDF5FormatNotFoundException();
		}

		// TODO: By default, we overwrite an existing HDF5 file -- may
		// want to consider "-force" flag
		try {
			fileHandle = (H5File) fileFormat.createFile(outPath,
														FileFormat.FILE_CREATE_DELETE);
			fileHandle = new H5File(outPath,
									FileFormat.FILE_CREATE_DELETE);

			fileHandle.open();
		} catch (Throwable t) {
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
	public void initializeFile() throws Exception {
		try {
			Datatype titleType = new H5Datatype(Datatype.CLASS_STRING,
											  1,
											  Datatype.ORDER_LE,
											  Datatype.SIGN_NONE);

			Attribute titleAttr = new Attribute("TITLE",
												titleType,
												new long[] {1},
												"Equity Data");

			fileHandle.writeAttribute(rootGroup, titleAttr, false);



		} catch (Throwable t) {

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
				Group toAdd = fileHandle.createGroup(ticker, rootGroup);
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
				fileHandle.close();
			} catch (Throwable t) {
				System.err.println("An exception occured while " +
								   "trying to save the current file: " +
								   t.toString());
				System.err.println("Failing to save.");
			}
		}
	}
}
