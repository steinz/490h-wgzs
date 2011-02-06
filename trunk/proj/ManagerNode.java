import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Map.Entry;

import edu.washington.cs.cse490h.lib.Utility;

//NOTE: Implicit transactions are handled by cache coherency!


// TODO: HIGH: TEST: If a client tries to write to a file that you locked previously, you should be granted automatic RW. This should work now, but testing...
// TODO: HIGH: TEST: Heartbeat pings, abort clients if they don't respond
// TODO: HIGH: Deal with creates and deletes appropriately -  
// 		for create, need to change W_DEL and createReceive to use createfiletx and deletefiletx.
// TODO: HIGH: In the case where the file doesn't exist, etc. should send an abort if client is in the middle of a tx.
// TODO: HIGH: Need to handle aborts, failures, and successes
// TODO: HIGH: Send txFailure in the case of errors (e.g. filenotfound)

public class ManagerNode {

	/**
	 * A list of locked files (cache coherency)
	 */
	private Map<String, Integer> LockedFiles;

	/**
	 * A map of queued file requests, from filename -> Client request
	 */
	private Map<String, Queue<QueuedFileRequest>> queuedFileRequests;

	/**
	 * A map of who has RW status on a given filename
	 */
	private Map<String, Integer> cacheRW;

	/**
	 * A map of who has RO status on a given filename
	 */
	private Map<String, List<Integer>> cacheRO;

	private Client node;
	/**
	 * List of nodes the manager is waiting for ICs from.
	 */
	protected Map<String, List<Integer>> pendingICs;

	/**
	 * Status of who is waiting for permission for this file
	 */
	private Map<String, Integer> pendingCCPermissionRequests;

	/**
	 * Status of who is waiting to delete this file via RPC
	 */
	private Map<String, Integer> pendingRPCDeleteRequests;

	/**
	 * Status of who is waiting to create this file via RPC
	 */
	private Map<String, Integer> pendingRPCCreateRequests;

	/**
	 * A set of node addresses currently performing transactions
	 * 
	 * TODO: HIGH: Could be pushed down into the TransactionalFileSystem
	 */
	protected Set<Integer> transactionsInProgress;

	public ManagerNode(Client n) {
		node = n;
		this.LockedFiles = new HashMap<String, Integer>();
		this.pendingICs = new HashMap<String, List<Integer>>();
		this.queuedFileRequests = new HashMap<String, Queue<QueuedFileRequest>>();
		this.pendingCCPermissionRequests = new HashMap<String, Integer>();
		this.pendingRPCDeleteRequests = new HashMap<String, Integer>();
		this.pendingRPCCreateRequests = new HashMap<String, Integer>();
		this.transactionsInProgress = new HashSet<Integer>();
	}

	public void createNewFile(String filename, int from) {
		// local create
		try {
			this.node.fs.createFile(filename);
		} catch (IOException e) {
			sendError(from, filename, e.getMessage());
		}
		// give RW to the requester for filename
		this.node.printVerbose("Changing status of client: " + from + " to RW for file: " + filename);
		cacheRW.put(filename, from);

		// send success to requester
		this.node.printVerbose("sending "
				+ Protocol.protocolToString(Protocol.SUCCESS) + " to " + from);
		sendSuccess(from, Protocol.CREATE, filename);
	}

	public void deleteExistingFile(String filename, int from) {
		// delete the file locally
		try {
			this.node.fs.deleteFile(filename);
		} catch (IOException e) {
			sendError(from, filename, e.getMessage());
		}
		// update permissions
		this.node.printVerbose("marking file " + filename + " as unowned");


		this.node.printVerbose("Blanking all permissions for file: " + filename);
		cacheRO.put(filename, new ArrayList<Integer>());
		cacheRW.put(filename, -1);

		// no one had permissions, so send success
		sendSuccess(from, Protocol.DELETE, filename);
	}


	/**
	 * NOTE for why this returns false if the file is locked by the client:
	 * If the client is the one locking the file, then it's fine to give them permission and not queue the request, because this patient probably has an exclusive lock on the file.
	 * This also means that even if someone else has RO on the file, then it's a good idea to just return and allow them to proceed as normal (since they'll revoke someone else's access
	 * if they're requesting ownership).
	 */
	/**
	 * Queues the given request if the file is locked and returns true. Returns
	 * false if the file isn't locked. Also returns false if the file is locked by the requesting client (i.e. the client is the one locking the file).
	 */
	public boolean queueRequestIfLocked(int client, int protocol,
			String filename) {
		if (LockedFiles.containsKey(filename)) {
			
			if (LockedFiles.get(filename).equals(client))
				return false;
			
			Queue<QueuedFileRequest> requests = queuedFileRequests
					.get(filename);
			if (requests == null) {
				requests = new LinkedList<QueuedFileRequest>();
				queuedFileRequests.put(filename, requests);
			}
			requests.add(new QueuedFileRequest(client, protocol, Utility
					.stringToByteArray(filename)));
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Returns who has current ownership of the file.
	 * 
	 * @param filename
	 *            The filename
	 * @return The client who owns this file, -1 if no one currently owns this
	 *         file.
	 * 
	 *         TODO: HIGH: I'd suggest returning null instead of -1 if nobody
	 *         has RW, some of the other code assumes it.
	 */
	public Integer checkRWClients(String filename) {

		Integer clientNumber = -1; // Return -1 if no clients have ownership of
									// this file

		if (cacheRW.containsKey(filename)) {
			clientNumber = cacheRW.get(filename);
		}

		return clientNumber;
	}

	/**
	 * Returns all clients that have RO status of the given file
	 * 
	 * @param filename
	 *            The filename
	 * @return The clients who have RO status on this file, may be empty if no
	 *         one has RO status.
	 */
	public List<Integer> checkROClients(String filename) {
		List<Integer> clients = new ArrayList<Integer>();

		if (cacheRO.containsKey(filename)) {
			clients = cacheRO.get(filename);
		}

		return clients;
	}

	public void receiveWD_DELETE(int from, String filename) {
		// delete locally
		try {
			this.node.fs.deleteFile(filename);
		} catch (IOException e) {
			sendError(from, filename, e.getMessage());
		}
		// remove permissions

		this.node.printVerbose("Blanking permissions for file: " + filename);
		cacheRW.put(filename, -1);
		cacheRO.put(filename, new ArrayList<Integer>());

		// look for pending requests

		// check for a create
		Integer requester = pendingRPCCreateRequests.remove(filename);
		if (requester != null) {
			// create the file which was deleted by the owner
			createNewFile(filename, requester);
			unlockFile(filename);
			return;
		}

		requester = pendingRPCDeleteRequests.remove(filename);
		if (requester != null) {
			// file was previously deleted by owner
			sendError(requester, Protocol.DELETE, filename,
					ErrorCode.FileDoesNotExist);
			return;
		}

		requester = pendingCCPermissionRequests.remove(filename);
		if (requester != null) {
			// file was deleted by owner
			sendError(requester, Protocol.DELETE, filename, ErrorCode.FileDoesNotExist);
			return;
		}

	}

	public void receiveRD(int from, String filename, String contents) {

		// first write the file to save a local copy
		try {
			this.node.fs.writeFile(filename, contents, false);
		} catch (IOException e) {
			sendError(from, filename, e.getMessage());
		}

		/*
		 * send out a RD to anyone requesting this - unlike for WD, this
		 * shouldn't be a create or delete (which require RW, so send W{F,D}
		 * messages instead)
		 */
		Integer destAddr = pendingCCPermissionRequests.remove(filename);
		if (destAddr != null) {
			try {
				sendFile(destAddr, filename, Protocol.RD);
			} catch (IOException e) {
				sendError(from, filename, e.getMessage());
			}

			// Add to RO list
			List<Integer> ro = cacheRO.get(filename);
			if (ro == null)
				ro = new ArrayList<Integer>();
			ro.add(destAddr);
		}

	}

	public void receiveWD(int from, String filename, String contents) {

		/*
		 * TODO: LOW: No need to do this for deletes or creates (although for
		 * creates it might give us a newer file version, which might be nice)
		 */
		// first write the file to save a local copy
		try {
			this.node.fs.writeFile(filename, contents, false);
		} catch (IOException e) {
			sendError(from, filename, e.getMessage());
		}

		// look for pending request
		boolean foundPendingRequest = false;

		// check creates
		Integer destAddr = pendingRPCCreateRequests.remove(filename);
		if (destAddr != null) {
			foundPendingRequest = true;
			sendError(destAddr, Protocol.CREATE, filename,
					ErrorCode.FileAlreadyExists);
		}

		// check deletes
		destAddr = pendingRPCDeleteRequests.remove(filename);
		if (destAddr != null) {
			if (foundPendingRequest) {
				sendError(from, filename, "IllegalConcurrentRequestException!");
			}
			foundPendingRequest = true;
			deleteExistingFile(filename, destAddr);
		}

		// check CC
		destAddr = pendingCCPermissionRequests.remove(filename);
		if (destAddr != null) {
			if (foundPendingRequest) {
				sendError(from, filename, "IllegalConcurrentRequestException!");
			}
			foundPendingRequest = true;
			try {
				sendFile(destAddr, filename, Protocol.WD);
			} catch (IOException e) {
				sendError(from, filename, e.getMessage());
			}
		}

		if (!foundPendingRequest) {

			sendError(from, filename, "MissingPendingRequestException: file: "
					+ filename);
		}

		// update the status of the client who sent the WD
		if (checkRWClients(filename) != from) {
			// TODO: HIGH: Throw error, send error, something
		} else{
			this.node.printVerbose("Blanking ownership permissions for file: " + filename);
			cacheRW.put(filename, -1);
		}
	}

	/**
	 * Helper the manager should use to lock a file
	 * 
	 * @param filename
	 */
	protected void lockFile(String filename, Integer from) {
		/**
		 * TODO: Detect if client is performing operations out of order during
		 * transaction.
		 */

		this.node.printVerbose("manager locking file: " + filename);
		this.node.logSynopticEvent("MANAGER-LOCK");
		LockedFiles.put(filename, from);
	}

	public void receiveTX_START(int from) {

		if (transactionsInProgress.contains(from)) {
			sendError(from, "", "tx already in progress on client");
		}

		transactionsInProgress.add(from);
		this.node.send(from, Protocol.HEARTBEAT, Utility.stringToByteArray(""));
	}

	public void receiveTX_COMMIT(int from) {
		if (!transactionsInProgress.contains(from)) {
			sendError(from, "", "tx not in progress on client");
		}

		try {
			this.node.fs.commitTransaction(from);
		} catch (IOException e) {
			sendError(from, "", e.getMessage());
		} catch (TransactionException e) {
			sendError(from, "", e.getMessage());
		}
		transactionsInProgress.remove(from); // remove from tx
		
		// unlock files
		for (Entry<String, Integer> entry: LockedFiles.entrySet()) {
			if (entry.getValue() == from){
				unlockFile(entry.getKey());
			}
		}
		
		this.node.RIOSend(from, Protocol.TX_SUCCESS, Client.emptyPayload);
		
		
		// stop doing heartbeat pings
	}

	public void receiveTX_ABORT(int from) {
		if (!transactionsInProgress.contains(from)) {
			sendError(from, "", "tx not in progress on client");
		}
		try {
			this.node.fs.abortTransaction(from);
		} catch (TransactionException e) {
			sendError(from, "", e.getMessage());
		} catch (IOException e) {
			sendError(from, "", e.getMessage());
		}
		transactionsInProgress.remove(from);

	}

	/**
	 * Queues the given request if the file is locked and returns true. Returns
	 * false if the file isn't locked.
	 */
	public boolean managerQueueRequestIfLocked(int client, int protocol,
			String filename) {
		if (LockedFiles.containsKey(filename)) {
			Queue<QueuedFileRequest> requests = queuedFileRequests
					.get(filename);
			if (requests == null) {
				requests = new LinkedList<QueuedFileRequest>();
				queuedFileRequests.put(filename, requests);
			}
			requests.add(new QueuedFileRequest(client, protocol, Utility
					.stringToByteArray(filename)));
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Create RPC
	 * 
	 */
	public void receiveCreate(int client, String filename) {

		if (queueRequestIfLocked(client, Protocol.CREATE, filename)) {
			return;
		}

		// Find out if anyone has RW
		Integer rw = checkRWClients(filename);
		List<Integer> ro = checkROClients(filename);

		if (rw != null) {
			sendRequest(rw, filename, Protocol.WF);
			pendingRPCCreateRequests.put(filename, client);
			lockFile(filename, client);
		} else if (ro.size() != 0) {
			// Someone has RO, so throw an error that the file exists already
			sendError(client, Protocol.ERROR, filename,
					ErrorCode.FileAlreadyExists);
		} else { // File not in system
			createNewFile(filename, client);
		}

	}

	public void receiveDelete(int from, String filename) {
		if (queueRequestIfLocked(from, Protocol.DELETE, filename)) {
			return;
		}

		Integer rw = checkRWClients(filename);
		List<Integer> ro = checkROClients(filename);

		if (rw == null && ro.size() == 0) {
			// File doesn't exist, send an error to the requester
			sendError(from, Protocol.DELETE, filename,
					ErrorCode.FileDoesNotExist);
		}

		boolean waitingForResponses = false;

		// add to pending ICs
		if (rw != null && rw == from) {
			// Requester should have RW
			sendError(from, "", "Got delete request from client with RW");
		} else if (rw != null && rw != from) {
			// Someone other than the requester has RW status, get updates
			sendRequest(rw, filename, Protocol.WF);
			waitingForResponses = true;
		} else if (ro.size() != 0) {
			pendingICs.put(filename, ro);
			for (Integer i : ro) {
				/*
				 * Send invalidate requests to everyone with RO (doesn't include
				 * the requester)
				 */
				sendRequest(i, filename, Protocol.IV);
			}
			waitingForResponses = true;
		}

		if (waitingForResponses) {
			// track pending request
			pendingRPCDeleteRequests.put(filename, from);
			lockFile(filename, from);
		} else {
			deleteExistingFile(filename, from);
		}
	}

	public void receiveQ(int from, String filename, int receivedProtocol,
			int responseProtocol, int forwardingProtocol, boolean preserveROs) {

		// check if locked
		if (queueRequestIfLocked(from, receivedProtocol, filename)) {
			/**
			 *  If the person who locked the file was the requester, then give them automatic
			 *  RW anyway.
			 */
			
			return;
		}

		// lock
		lockFile(filename, from);

		// address of node w/ rw or null
		Integer rw = checkRWClients(filename);
		List<Integer> ro = checkROClients(filename);

		if (rw == null && ro.size() == 0) {
			sendError(from, Protocol.ERROR, filename,
					ErrorCode.FileDoesNotExist);
		}

		if (rw != null && ro.size() > 0) {
			sendError(from, filename, "simultaneous RW (" + rw + ") and ROs ("
					+ ro.toString() + ") detected on file: " + filename);
		}

		if (rw != null) { // someone has RW
			// Get updates
			sendRequest(rw, filename, forwardingProtocol);
			pendingCCPermissionRequests.put(filename, from);
		} else if (!preserveROs && ro.size() > 0) { // someone(s) have RO
			pendingICs.put(filename, ro);
			for (int i : ro) {
				// Invalidate all ROs
				sendRequest(i, filename, Protocol.IV);
			}
			pendingCCPermissionRequests.put(filename, from);
		} else { // no one has RW or RO
			/*
			 * TODO: Should this be an error - I think it currently sends
			 * whatever is on disk?
			 */

			// send file to requester
			try {
				sendFile(from, filename, responseProtocol);
			} catch (IOException e) {
				sendError(from, filename, e.getMessage());
			}
			// unlock and priveleges updated by C message handlers
		}
	}

	/**
	 * 
	 * @param from
	 *            The node this IC was received from.
	 * @param filename
	 *            Should be the file name. Throws an error if we were not
	 *            waiting for an IC from this node for this file
	 */
	public void receiveIC(Integer from, String filename) {

		/*
		 * TODO: Maybe different messages for the first two vs. the last
		 * scenario (node is manager but not expecting IC from this node for
		 * this file)?
		 */

		int destAddr;
		if (!pendingICs.containsKey(filename)
				|| !pendingICs.get(filename).contains(from)) {
			sendError(from, Protocol.ERROR, filename, ErrorCode.UnknownError);
		} else {

			// update the status of the client who sent the IC
			List<Integer> ro = checkROClients(filename);
			ro.remove(filename);
			this.node.printVerbose("Changing status of client: " + from + " to RO for file: " + filename);
			cacheRO.put(filename, ro);

			this.node.printVerbose("Changing client: " + from + " to IV");

			List<Integer> waitingForICsFrom = pendingICs.get(filename);

			waitingForICsFrom.remove(from);
			if (waitingForICsFrom.isEmpty()) {
				/*
				 * If the pending ICs are now empty, someone's waiting for a WD,
				 * so check for that and send
				 */

				/*
				 * TODO: this should just clear to prevent reinitialization
				 * maybe, although this way could save some memory... Anyway,
				 * check that whatever assumption is made holds
				 */
				pendingICs.remove(filename);

				if (pendingCCPermissionRequests.containsKey(filename)) {
					destAddr = pendingCCPermissionRequests.remove(filename);
					try {
						sendFile(destAddr, filename, Protocol.WD);
					} catch (IOException e) {
						sendError(from, filename, e.getMessage());
					}
				} else {
					destAddr = pendingRPCDeleteRequests.remove(filename);
					sendSuccess(destAddr, Protocol.DELETE, filename);
				}
			} else {
				// still waiting for more ICs
				List<Integer> waitingFor = pendingICs.get(filename);
				StringBuilder waiting = new StringBuilder();
				waiting.append("Received IC but waiting for IC from clients : ");
				for (int i : waitingFor) {
					waiting.append(i + " ");
				}
				this.node.printVerbose(waiting.toString());
			}
		}
	}

	public void receiveRC(int from, String filename) {
		this.node.printVerbose("Changing client: " + from + " to RO");

		List<Integer> ro = cacheRO.get(filename);
		ro.add(from);
		if (checkRWClients(filename) == from){
			this.node.printVerbose("Blanking ownership permissions for file: " + filename);
			cacheRW.put(filename, -1);
		}

		// check if someone's in the middle of a transaction with this file. if
		// so, don't do anything.
		if (!LockedFiles.containsKey(filename)
				|| !transactionsInProgress.contains(LockedFiles.get(filename)))
			unlockFile(filename);

	}

	public void receiveWC(int from, String filename) {

		// TODO: Check if someone has RW already, throw error if so
		this.node.printVerbose("Changing status of client: " + from + " to RW for file: " + filename);
		cacheRW.put(filename, from);

		// check if someone's in the middle of a transaction with this file. if
		// so, don't do anything.
		if (!LockedFiles.containsKey(filename)
				|| !transactionsInProgress.contains(LockedFiles.get(filename)))
			unlockFile(filename);
	}

	/**
	 * Helper that sends the contents of filename to to with protocol protocol.
	 * Should only be used by the manager.
	 * 
	 * @throws IOException
	 */
	protected void sendFile(int to, String filename, int protocol)
			throws IOException {
		StringBuilder sendMsg = new StringBuilder();

		if (!Utility.fileExists(this.node, filename)) {
			// Manager doesn't have the file
			sendError(to, Protocol.ERROR, filename, ErrorCode.FileDoesNotExist);
		} else {
			sendMsg.append(filename);
			sendMsg.append(Client.delimiter);
			sendMsg.append(this.node.fs.getFile(filename));
		}

		byte[] payload = Utility.stringToByteArray(sendMsg.toString());
		this.node.printVerbose("sending " + Protocol.protocolToString(protocol)
				+ " to " + to);
		this.node.RIOSend(to, protocol, payload);
	}

	/**
	 * Unlocks filename and checks if there is another request to service
	 */
	protected void unlockFile(String filename) {

		this.node.printVerbose("manager unlocking file: " + filename);
		this.node.logSynopticEvent("MANAGER-UNLOCK");
		LockedFiles.remove(filename);

		Queue<QueuedFileRequest> outstandingRequests = queuedFileRequests
				.get(filename);
		while (outstandingRequests != null) {
			QueuedFileRequest nextRequest = outstandingRequests.poll();
			if (nextRequest != null) {
				this.node.onRIOReceive(nextRequest.from, nextRequest.protocol,
						nextRequest.msg);
			}
		}
	}

	/**
	 * Helper that sends a request for the provided filename to the provided
	 * client using the provided protocol
	 */
	protected void sendRequest(int client, String filename, int protocol) {
		byte[] payload = Utility.stringToByteArray(filename);
		node.RIOSend(client, protocol, payload);
	}

	/*
	 * TODO: Wayne: Comment all send{Success, Error} methods and think about
	 * wheter or not we really need all of them
	 */

	/*
	 * TODO: send successess and errors for all ops done outside of txs, and
	 * tx_successful tx_failure at end of all txs (w/ no op specific responses)?
	 */

	protected void sendSuccess(int destAddr, int protocol, String message) {
		String msg = Protocol.protocolToString(protocol) + Client.delimiter
				+ message;
		byte[] payload = Utility.stringToByteArray(msg);
		this.node.RIOSend(destAddr, Protocol.SUCCESS, payload);
	}

	/**
	 * Send Error method
	 * 
	 * @param destAddr
	 *            Who to send the error code to
	 * @param protocol
	 *            The protocol that failed
	 * @param filename
	 *            The filename for the protocol that failed
	 * @param errorcode
	 *            The error code
	 */
	protected void sendError(int destAddr, int protocol, String filename,
			int errorcode) {
		String msg = filename + Client.delimiter
				+ Protocol.protocolToString(protocol) + Client.delimiter
				+ ErrorCode.lookup(errorcode);
		byte[] payload = Utility.stringToByteArray(msg);
		this.node.RIOSend(destAddr, Protocol.ERROR, payload);
	}

	public void sendError(int from, String filename, String message) {
		String msg = filename + Client.delimiter + message;
		byte[] payload = Utility.stringToByteArray(msg);
		this.node.RIOSend(from, Protocol.ERROR, payload);
	}

	/**
	 * This packet timed out and was a heartbeat packet. It may have been acked, or it may
	 * not have - it's irrelevant from the point of view of the manager.
	 * @param destAddr the destination address for the heartbeat packet
	 */
	public void heartbeatTimeout(int destAddr) {
		if (transactionsInProgress.contains(destAddr))
			this.node.RIOSend(destAddr, Protocol.HEARTBEAT, Utility.stringToByteArray(""));
		
	}
	
	/**
	 * This node didn't respond to a packet even after the maximum number of tries. If this client
	 * was in the middle of the transaction, they're now aborted and all locks on files they
	 * own are released
	 * @param destAddr
	 */
	public void killNode(int destAddr){
		// might as well send a txabort just in case this node is alive
		this.node.RIOSend(destAddr, Protocol.TX_ABORT, Utility.stringToByteArray(""));
		transactionsInProgress.remove(destAddr);
		for (Entry<String, Integer> entry : LockedFiles.entrySet())
		{
			if (entry.getValue().equals(destAddr))
				unlockFile(entry.getKey());
		}
	}

}
