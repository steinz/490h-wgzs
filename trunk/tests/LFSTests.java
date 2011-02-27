import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.washington.cs.cse490h.tdfs.FileDoesNotExistException;
import edu.washington.cs.cse490h.tdfs.LogFileSystem;

public class LFSTests {

	LogFileSystem fs;

	@Before
	public void setUp() throws Exception {
		fs = new LogFileSystem();
	}

	@After
	public void tearDown() throws Exception {
		fs = null;
	}

	@Test
	public void lockTest() {
		String filename = "test";
		int address = 0;

		try {
			fs.createGroup(filename);
			fs.createFile(filename, address);
			fs.lockFile(filename, address);
			assertEquals(new Integer(0), fs.checkLocked(filename));
			fs.unlockFile(filename, address);
			assertEquals(null, fs.checkLocked(filename));
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	@Test
	public void packCreateGroupTest() {
		try {
			String filename = "test";

			fs.createGroup(filename);
			byte[] packed = fs.packLog(filename);

			fs = new LogFileSystem();
			fs.joinGroup(filename, packed);

			assertEquals(false, fs.fileExists(filename));
			try {
				fs.getFile(filename);
				fail("expect FileDoesNotExistException");
			} catch (FileDoesNotExistException e) {
			}
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	@Test
	public void packCreateFileTest() {
		try {
			String filename = "test";
			int address = 0;

			fs.createGroup(filename);
			fs.createFile(filename, address);
			byte[] packed = fs.packLog(filename);

			fs = new LogFileSystem();
			fs.joinGroup(filename, packed);
			String got = fs.getFile(filename);

			assertEquals(true, fs.fileExists(filename));
			assertEquals("", fs.getFile(filename));
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	@Test
	public void packWriteFileTest() {
		try {
			String filename = "test";
			int address = 0;
			String content = "hello world";

			fs.createGroup(filename);
			fs.createFile(filename, address);
			fs.writeFile(filename, content, false, address);
			byte[] packed = fs.packLog(filename);

			fs = new LogFileSystem();
			fs.joinGroup(filename, packed);
			String got = fs.getFile(filename);

			assertEquals(true, fs.fileExists(filename));
			assertEquals(content, got);
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	@Test
	public void packDeleteTest() {
		try {
			String filename = "test";
			int address = 0;
			String content = "hello world";

			fs.createGroup(filename);
			fs.createFile(filename, address);
			fs.writeFile(filename, content, false, address);
			fs.deleteFile(filename, address);
			byte[] packed = fs.packLog(filename);

			fs = new LogFileSystem();
			fs.joinGroup(filename, packed);

			assertEquals(false, fs.fileExists(filename));
			try {
				fs.getFile(filename);
				fail("expect FileDoesNotExistException");
			} catch (FileDoesNotExistException e) {
			}
		} catch (Exception e) {
			fail(e.toString());
		}
	}
}
