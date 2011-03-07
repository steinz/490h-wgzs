package edu.washington.cs.cse490h.tdfs.tests;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import edu.washington.cs.cse490h.lib.PersistentStorageInputStream;
import edu.washington.cs.cse490h.lib.PersistentStorageOutputStream;
import edu.washington.cs.cse490h.tdfs.TDFSNode;

class TDFSNodeStub extends TDFSNode {
	@Override
	public PersistentStorageOutputStream getOutputStream(String filename,
			boolean append) throws IOException {
		return new PersistentStorageOutputStream(this, new File(filename
				+ ".tdfs_stub"), append);
	}

	@Override
	public PersistentStorageInputStream getInputStream(String filename)
			throws FileNotFoundException {
		return new PersistentStorageInputStream(this, filename + ".tdfs_stub");
	}

	@Override
	public void handleDiskWriteEvent(String description, String synDescription) {
		return;
	}
	
	@Override
	public void handleDiskReadEvent(String synDescription) {
		return;
	}
}
