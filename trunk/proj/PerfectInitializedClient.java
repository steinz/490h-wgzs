/**
 * CSE 490h
 * @author wayger, steinz
 */

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Extension of the PerfectClient with rigged UUIDs so we can bypass handshakes.
 * Only works for up to MAX_CLIENT_COUNT clients w/ addresses
 * 0-MAX_CLIENT_COUNT.
 */
public class PerfectInitializedClient extends PerfectClient {

	/**
	 * number of clients for everyone to pre-initialize handshakes with. uuid
	 * setting assumes this is at most 9
	 */
	protected final int MAX_CLIENT_COUNT = 4;

	public void start() {
		super.start();

		this.ID = UUID.fromString("0000000-0000-0000-0000-00000000000"
				+ this.addr);
		for (int i = 0; i < MAX_CLIENT_COUNT; i++) {
			this.addrToSessionIDMap.put(i,
					UUID.fromString("0000000-0000-0000-0000-00000000000" + i));
		}

		this.managerAddr = 0;
		if (this.addr == 0) {
			this.isManager = true;
			this.managerLockedFiles = new HashSet<String>();
			this.managerCacheStatuses = new HashMap<String, Map<Integer, CacheStatuses>>();
			this.managerPendingICs = new HashMap<String, List<Integer>>();
		}
		
		printVerbose("initialized with addr: " + this.addr + " uuid: "
				+ this.ID);
	}
}
