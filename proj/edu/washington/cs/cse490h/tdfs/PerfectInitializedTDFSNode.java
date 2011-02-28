package edu.washington.cs.cse490h.tdfs;

/**
 * CSE 490h
 * @author wayger, steinz
 */

import java.util.UUID;

/**
 * Extension of the PerfectTDFSNode with rigged UUIDs so we can bypass
 * handshakes. Assumes the manager has address 0. Only works for up to
 * MAX_CLIENT_COUNT clients w/ addresses 0-MAX_CLIENT_COUNT.
 */
public class PerfectInitializedTDFSNode extends PerfectTDFSNode {

	/**
	 * number of clients for everyone to pre-initialize handshakes with. uuid
	 * setting assumes this is at most 10
	 */
	protected final int MAX_CLIENT_COUNT = 10;

	public void start() {
		super.start();

		// Setup UUID mappings
		this.ID = UUID.fromString("0000000-0000-0000-0000-00000000000"
				+ this.addr);
		for (int i = 0; i < MAX_CLIENT_COUNT; i++) {
			this.addrToSessionIDMap.put(i,
					UUID.fromString("0000000-0000-0000-0000-00000000000" + i));
		}

		if (this.addr == 0) {
			printInfo("initialized as manager with session id: " + this.ID);
		} else {
			printInfo("initialized as client with session id: " + this.ID);
		}
	}
}
