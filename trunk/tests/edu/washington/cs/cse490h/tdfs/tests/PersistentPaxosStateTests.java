package edu.washington.cs.cse490h.tdfs.tests;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.washington.cs.cse490h.tdfs.LogEntry;
import edu.washington.cs.cse490h.tdfs.PersistentPaxosState;
import edu.washington.cs.cse490h.tdfs.Proposal;

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

	@Test
	public void zeroAccept() {
		assertNull(paxosState.highestAccepted("f", -1));

		paxosState = null;
		paxosState = new PersistentPaxosState(n);

		assertNull(paxosState.highestAccepted("f", -1));
	}

	@Test
	public void oneAccept() {
		LogEntry e1 = new LogEntry() {
			@Override
			public String toString() {
				return "Create";
			}
		};
		Proposal p1 = new Proposal(e1, "f", 1, 1);
		paxosState.accept(p1);

		Proposal highest = paxosState.highestAccepted("f", 1);
		assertTrue(highest.filename == p1.filename);
		assertTrue(highest.operation.toString().equals(p1.operation.toString()));
		assertTrue(highest.operationNumber == p1.operationNumber);
		assertTrue(highest.proposalNumber == p1.proposalNumber);

		paxosState = null;
		paxosState = new PersistentPaxosState(n);

		highest = paxosState.highestAccepted("f", 1);
		assertTrue(highest.filename.equals(p1.filename));
		assertTrue(highest.operation.toString().equals(p1.operation.toString()));
		assertTrue(highest.operationNumber == p1.operationNumber);
		assertTrue(highest.proposalNumber == p1.proposalNumber);
	}

	@Test
	public void twoAccepts() {
		LogEntry e1 = new LogEntry() {
			@Override
			public String toString() {
				return "Create";
			}
		};
		LogEntry e2 = new LogEntry() {
			@Override
			public String toString() {
				return "Delete";
			}
		};
		Proposal p1 = new Proposal(e1, "f", 1, 1);
		Proposal p2 = new Proposal(e2, "f", 1, 2);
		paxosState.accept(p1);
		paxosState.accept(p2);

		Proposal highest = paxosState.highestAccepted("f", 1);
		assertTrue(highest.filename.equals(p2.filename));
		assertTrue(highest.operation.toString().equals(p2.operation.toString()));
		assertTrue(highest.operationNumber == p2.operationNumber);
		assertTrue(highest.proposalNumber == p2.proposalNumber);

		paxosState = null;
		paxosState = new PersistentPaxosState(n);

		highest = paxosState.highestAccepted("f", 1);
		assertTrue(highest.filename.equals(p2.filename));
		assertTrue(highest.operation.toString().equals(p2.operation.toString()));
		assertTrue(highest.operationNumber == p2.operationNumber);
		assertTrue(highest.proposalNumber == p2.proposalNumber);
	}

	@Test
	public void twoAcceptsOutOfOrder() {
		LogEntry e1 = new LogEntry() {
			@Override
			public String toString() {
				return "Create";
			}
		};
		LogEntry e2 = new LogEntry() {
			@Override
			public String toString() {
				return "Delete";
			}
		};
		Proposal p1 = new Proposal(e1, "f", 1, 1);
		Proposal p2 = new Proposal(e2, "f", 1, 2);
		paxosState.accept(p2);
		paxosState.accept(p1);

		Proposal highest = paxosState.highestAccepted("f", 1);
		assertTrue(highest.filename.equals(p2.filename));
		assertTrue(highest.operation.toString().equals(p2.operation.toString()));
		assertTrue(highest.operationNumber == p2.operationNumber);
		assertTrue(highest.proposalNumber == p2.proposalNumber);

		paxosState = null;
		paxosState = new PersistentPaxosState(n);

		highest = paxosState.highestAccepted("f", 1);
		assertTrue(highest.filename.equals(p2.filename));
		assertTrue(highest.operation.toString().equals(p2.operation.toString()));
		assertTrue(highest.operationNumber == p2.operationNumber);
		assertTrue(highest.proposalNumber == p2.proposalNumber);
	}
}
