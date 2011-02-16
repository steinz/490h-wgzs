import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.washington.cs.cse490h.dfs.DFSNode;
import edu.washington.cs.cse490h.dfs.DFSException;
import edu.washington.cs.cse490h.dfs.TransactionalFileSystem;
import edu.washington.cs.cse490h.lib.Manager;
import edu.washington.cs.cse490h.lib.Utility;

public class TFSTests {

	DFSNodeStub n;
	TransactionalFileSystem fs;
	String filename = "test";
	String putContent = "no newline";
	String putContentNewline = "has newline\n";

	@Before
	public void setUp() throws Exception {
		Manager manager = new SimulatorStub(DFSNode.class, 10L, "", "");
		n = new DFSNodeStub();
		n.init(manager, 0);

		fs = new TransactionalFileSystem(n, ".t", ".l", ".l.t",
				Integer.MAX_VALUE);

		if (Utility.fileExists(n, filename)) {
			fs.deleteFile(filename);
		}
	}
	
	@After
	public void tearDown() throws Exception {
		fs.deleteFile(".l");
		fs.deleteFile(filename);
	}

	@Test
	public void createFile() {
		try {
			fs.startTransaction(n.addr);
			fs.createFileTX(n.addr, filename);
			fs.commitTransaction(n.addr);
			assertEquals(fs.getFile(filename), "");
		} catch (Exception e) {
			fail();
		}
	}

	private void createAndPutFile(boolean twoTxs, boolean newline)
			throws DFSException, IOException {
		fs.startTransaction(n.addr);
		fs.createFileTX(n.addr, filename);
		if (twoTxs) {
			fs.commitTransaction(n.addr);
			fs.startTransaction(n.addr);
		}
		fs.writeFileTX(n.addr, filename, newline ? putContentNewline
				: putContent, false);
		fs.commitTransaction(n.addr);
	}

	@Test
	public void createAndPutFileNoNewline2TX() {
		try {
			createAndPutFile(true, false);
			assertEquals(fs.getFile(filename), putContent);
		} catch (Exception e) {
			fail();
		}
	}

	@Test
	public void createAndPutFileNoNewline1TX() {
		try {
			createAndPutFile(false, false);
			assertEquals(fs.getFile(filename), putContent);
		} catch (Exception e) {
			fail();
		}
	}

	@Test
	public void createAndPutFileNewline2TX() {
		try {
			createAndPutFile(true, true);
			assertEquals(fs.getFile(filename), putContentNewline);
		} catch (Exception e) {
			fail();
		}
	}

	@Test
	public void createAndPutFileNewline1TX() {
		try {
			createAndPutFile(false, true);
			assertEquals(fs.getFile(filename), putContentNewline);
		} catch (Exception e) {
			fail();
		}
	}
}
