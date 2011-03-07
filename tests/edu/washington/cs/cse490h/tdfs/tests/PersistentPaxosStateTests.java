package edu.washington.cs.cse490h.tdfs.tests;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.washington.cs.cse490h.tdfs.PersistentPaxosState;

public class PersistentPaxosStateTests {

	TDFSNodeStub n;
	PersistentPaxosState paxosState;

	@Before
	public void setUp() {
		n = new TDFSNodeStub();
		paxosState = new PersistentPaxosState(n);
	}
	
	@After
	public void tearDown() throws IOException {
		paxosState.close();
		// TODO: HIGH: I have NO IDEA why this doesn't work...
		new File("accepted.log.tdfs_stub").delete();
		new File("promised.log.tdfs_stub").delete();
	}

	@Test
	public void zeroPromise() {
		assertNull(paxosState.highestPromised("f", -1));
		
		paxosState = null;
		paxosState = new PersistentPaxosState(n);
		
		assertNull(paxosState.highestPromised("f", -1));
	}

	@Test
	public void onePromise() {
		paxosState.promise("f", 1, 1);
		assertTrue(paxosState.highestPromised("f", 1) == 1);

		paxosState = null;
		paxosState = new PersistentPaxosState(n);

		assertTrue(paxosState.highestPromised("f", 1) == 1);
	}

	@Test
	public void twoPromises() {
		paxosState.promise("f", 1, 1);
		paxosState.promise("f", 1, 2);
		assertTrue(paxosState.highestPromised("f", 1) == 2);
		
		paxosState = null;
		paxosState = new PersistentPaxosState(n);

		assertTrue(paxosState.highestPromised("f", 1) == 2);
	}
	
	@Test
	public void twoPromisesOutOfOrder() {
		paxosState.promise("f", 1, 2);
		paxosState.promise("f", 1, 1);
		assertTrue(paxosState.highestPromised("f", 1) == 2);
		
		paxosState = null;
		paxosState = new PersistentPaxosState(n);

		assertTrue(paxosState.highestPromised("f", 1) == 2);
	}
}
