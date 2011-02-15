
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import edu.washington.cs.cse490h.lib.Manager;
import edu.washington.cs.cse490h.lib.Utility;


public class RFSTests {

	DFSNodeStub n;
	ReliableFileSystem fs;
	String filename = "test";
	String putContent = "no newline";
	String putContentNewline = "has newline\n";

	@Before
	public void setUp() throws Exception {
		Manager manager = new SimulatorStub(DFSNode.class, 10L, "", "");
		n = new DFSNodeStub();
		n.init(manager, 0);

		fs = new ReliableFileSystem(n, ".t");

		if (Utility.fileExists(n, filename)) {
			fs.deleteFile(filename);
		}
	}

	@Test
	public void createFile() {
		try {
			fs.createFile(filename);
			assertEquals(fs.getFile(filename), "");
		} catch (Exception e) {
			fail();
		}
	}

	private void createAndPutFile(boolean twoTxs, boolean newline)
			throws TransactionException, IOException {
		fs.createFile(filename);
		fs.writeFile(filename, newline ? putContentNewline
				: putContent, false);
	}

	@Test
	public void createAndPutFileNoNewline() {
		try {
			createAndPutFile(false, false);
			assertEquals(fs.getFile(filename), putContent);
		} catch (Exception e) {
			fail();
		}
	}

	@Test
	public void createAndPutFileNewline() {
		try {
			createAndPutFile(false, true);
			assertEquals(fs.getFile(filename), putContentNewline);
		} catch (Exception e) {
			fail();
		}
	}

}
