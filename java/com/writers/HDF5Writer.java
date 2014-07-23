package com.writers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import ncsa.hdf.object.FileFormat;
import ncsa.hdf.object.Group;
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

	public void run(){
		DataPoint dataPoint;

		while(inQueue.acceptingOrders || !inQueue.isEmpty()) {
			if((dataPoint = inQueue.deq()) != null) {

			}

		}
	}
}
