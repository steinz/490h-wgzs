import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;


import edu.washington.cs.cse490h.lib.Utility;

/**
 *  TODO: HIGH: All manager public functions need to take care of responding to client and logging
 *  as well. All should end with a response.
 */

// TODO: HIGH: Log all cache status changes if they're not logged already
// TODO: HIGH: Should not remove locks on file for clients for all ops in transactions until txcomplete. BUT: If a client tries to write 
//             to a file that you locked previously, you should be granted automatic RW. 
// TODO: HIGH: Heartbeat pings, abort clients if they don't respond
// 		Implicit transactions are handled by cache coherency!
// TODO: HIGH: Deal with creates and deletes appropriately -  
// 		for create, need to change W_DEL and createReceive to use createfiletx and deletefiletx.

public class ManagerNode {

	/**
	 * A list of locked files (cache coherency)
	 */
	private Set<String> LockedFiles;
	
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
	 * Files that are locked by transactions currently, mapped from filename -> destAddr
	 */
	private Map<String, Integer> transactionLockedFiles;

	/**
	 * A set of node addresses currently performing transactions
	 * 
	 * TODO: HIGH: Could be pushed down into the TransactionalFileSystem
	 */
	protected Set<Integer> managerTransactionsInProgress;
	
	
	public ManagerNode(Client n)
	{
		node = n;
		this.LockedFiles = new HashSet<String>();
		this.pendingICs = new HashMap<String, List<Integer>>();
		this.queuedFileRequests = new HashMap<String, Queue<QueuedFileRequest>>();
		this.pendingCCPermissionRequests = new HashMap<String, Integer>();
		this.pendingRPCDeleteRequests = new HashMap<String, Integer>();
		this.pendingRPCCreateRequests = new HashMap<String, Integer>();
		this.transactionLockedFiles = new HashMap<String, Integer>();
	}
	

	public void createNewFile(String filename, int from){
		// local create
		try{
			this.node.fs.createFile(filename);
		} catch (IOException e){
			this.node.sendError(from, filename, e.getMessage());
		}
		// give RW to the requester for filename
		cacheRW.put(filename, from);

		// send success to requester
		this.node.printVerbose("sending " + Protocol.protocolToString(Protocol.SUCCESS)
				+ " to " + from);
		this.node.sendSuccess(from, Protocol.CREATE, filename);
	}
	
	public void deleteExistingFile(String filename, int from){
		// delete the file locally
		try{
			this.node.fs.deleteFile(filename);
		} catch (IOException e){
			this.node.sendError(from, filename, e.getMessage());
		}
		// update permissions
		this.node.printVerbose("marking file " + filename + " as unowned");
		
		cacheRO.put(filename, new ArrayList<Integer>());
		cacheRW.put(filename, -1);

		// no one had permissions, so send success
		this.node.sendSuccess(from, Protocol.DELETE, filename);
	}
	/**
	 * Queues the given request if the file is locked and returns true. Returns
	 * false if the file isn't locked.
	 */
	public boolean queueRequestIfLocked(int client, int protocol,
			String filename) {
		if (LockedFiles.contains(filename)) {
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
	 * @param filename The filename
	 * @return The client who owns this file, -1 if no one currently owns this file.
	 */
	public Integer checkRWClients(String filename){
		
		Integer clientNumber = -1; // Return -1 if no clients have ownership of this file
		
		if (cacheRW.containsKey(filename)){
			clientNumber = cacheRW.get(filename);
		}
		
		return clientNumber;
	}
	
	/**
	 * Returns all clients that have RO status of the given file
	 * @param filename The filename
	 * @return The clients who have RO status on this file, may be empty if no one has RO status.
	 */
	public List<Integer> checkROClients(String filename){
		List<Integer> clients = new ArrayList<Integer>();
		
		if (cacheRO.containsKey(filename)){
			clients = cacheRO.get(filename);
		}
		
		return clients;
	}

	public void receiveWD_DELETE(int from, String filename){
		// delete locally
		try{
			this.node.fs.deleteFile(filename);
		} catch (IOException e)
		{
			this.node.sendError(from, filename, e.getMessage());
		}
		// remove permissions
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
			this.node.sendError(requester, Protocol.DELETE, filename,
					ErrorCode.FileDoesNotExist);
			return;
		}

		requester = pendingCCPermissionRequests.remove(filename);
		if (requester != null) {
			// file was deleted by owner
			this.node.sendError(requester, filename, ErrorCode.FileDoesNotExist);
			return;
		}

	}
	
	public void receiveRD(int from, String filename, String contents){

		// first write the file to save a local copy
		try {
			this.node.fs.writeFile(filename, contents, false);
		} catch (IOException e) {
			this.node.sendError(from, filename, e.getMessage());
		}

		/*
		 * send out a RD to anyone requesting this - unlike for WD, this
		 * shouldn't be a create or delete (which require RW, so send W{F,D}
		 * messages instead)
		 */
		Integer destAddr = pendingCCPermissionRequests.remove(filename);
		if (destAddr != null){
			try {
				sendFile(destAddr, filename, Protocol.RD);
			} catch (IOException e) {
				this.node.sendError(from, filename, e.getMessage());
			}

			// Add to RO list
			List<Integer> ro = cacheRO.get(filename);
			if (ro == null)
				ro = new ArrayList<Integer>();
			ro.add(destAddr);
		}
		
	}

	public void receiveWD(int from, String filename, String contents){
		
		/*
		 * TODO: LOW: No need to do this for deletes or creates (although
		 * for creates it might give us a newer file version, which might be
		 * nice)
		 */
		// first write the file to save a local copy
		try {
			this.node.fs.writeFile(filename, contents, false);
		} catch (IOException e) {
			this.node.sendError(from, filename, e.getMessage());
		}

		// look for pending request
		boolean foundPendingRequest = false;

		// check creates
		Integer destAddr = pendingRPCCreateRequests.remove(filename);
		if (destAddr != null) {
			foundPendingRequest = true;
			this.node.sendError(destAddr, Protocol.CREATE, filename,
					ErrorCode.FileAlreadyExists);
		}

		// check deletes
		destAddr = pendingRPCDeleteRequests.remove(filename);
		if (destAddr != null) {
			if (foundPendingRequest) {
				this.node.sendError(from, filename, "IllegalConcurrentRequestException!");
			}
			foundPendingRequest = true;
			deleteExistingFile(filename, destAddr);
		}

		// check CC
		destAddr = pendingCCPermissionRequests.remove(filename);
		if (destAddr != null) {
			if (foundPendingRequest) {
				this.node.sendError(from, filename, "IllegalConcurrentRequestException!");
			}
			foundPendingRequest = true;
			try {
				sendFile(destAddr, filename, Protocol.WD);
			} catch (IOException e) {
				this.node.sendError(from, filename, e.getMessage());
			}
		}

		if (!foundPendingRequest) {

			this.node.sendError(from, filename, "MissingPendingRequestException: file: " + filename);
		}

		// update the status of the client who sent the WD
		if (checkRWClients(filename) != from){
			// TODO: HIGH: Throw error, send error, something
		} else
			cacheRW.put(filename, -1);
	}
	/**
	 * Helper the manager should use to lock a file
	 * 
	 * @param filename
	 */
	protected void lockFile(String filename) {
		/**
		 * TODO: Detect if client is performing operations out of order during
		 * transaction.
		 */

		this.node.printVerbose("manager locking file: " + filename);
		this.node.logSynopticEvent("MANAGER-LOCK");
		LockedFiles.add(filename);
	}
	
	public void receiveTX_START(int from) {
		
		if (managerTransactionsInProgress.contains(from)) {
			this.node.sendError(from, "", "tx already in progress on client");
		}

		managerTransactionsInProgress.add(from);
	}
	
	public void receiveTX_COMMIT(int from){
		if (!managerTransactionsInProgress.contains(from)) {
			this.node.sendError(from, "", "tx not in progress on client");
		}

		try {
			this.node.fs.commitTransaction(from);
		} catch (IOException e) {
			this.node.sendError(from, "", e.getMessage());
		} catch (TransactionException e) {
			this.node.sendError(from, "", e.getMessage());
		}
		managerTransactionsInProgress.remove(from);
		this.node.RIOSend(from, Protocol.TX_SUCCESS, Client.emptyPayload);
	}

	public void receiveTX_ABORT(int from){
		if (!managerTransactionsInProgress.contains(from)) {
			this.node.sendError(from, "", "tx not in progress on client");
		}
		try {
			this.node.fs.abortTransaction(from);
		} catch (TransactionException e) {
			this.node.sendError(from, "", e.getMessage());
		} catch (IOException e) {
			this.node.sendError(from, "", e.getMessage());
		}
		managerTransactionsInProgress.remove(from);
		
	}
	
	/**
	 * Queues the given request if the file is locked and returns true. Returns
	 * false if the file isn't locked.
	 */
	public boolean managerQueueRequestIfLocked(int client, int protocol,
			String filename) {
		if (LockedFiles.contains(filename)) {
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
			this.node.sendRequest(rw, filename, Protocol.WF);
			pendingRPCCreateRequests.put(filename, client);
			lockFile(filename);
		} else if (ro.size() != 0)
		{
			// Someone has RO, so throw an error that the file exists already
			this.node.sendError(client, Protocol.ERROR, filename, ErrorCode.FileAlreadyExists);
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
			this.node.sendError(from, Protocol.DELETE, filename, ErrorCode.FileDoesNotExist);
		}

		boolean waitingForResponses = false;

		// add to pending ICs
		if (rw != null && rw == from) {
			// Requester should have RW
			this.node.sendError(from, "", "Got delete request from client with RW");
		} else if (rw != null && rw != from) {
			// Someone other than the requester has RW status, get updates
			this.node.sendRequest(rw, filename, Protocol.WF);
			waitingForResponses = true;
		} else if (ro.size() != 0) {
			pendingICs.put(filename, ro);
			for (Integer i : ro) {
				/*
				 * Send invalidate requests to everyone with RO (doesn't include
				 * the requester)
				 */
				this.node.sendRequest(i, filename, Protocol.IV);
			}
			waitingForResponses = true;
		}

		if (waitingForResponses) {
			// track pending request
			pendingRPCDeleteRequests.put(filename, from);
			lockFile(filename);
		} else {
			deleteExistingFile(filename, from);
		}
	}

	public void receiveQ(int from, String filename, int receivedProtocol, 
			int responseProtocol, int forwardingProtocol, boolean preserveROs){
		
		// check if locked
		if (queueRequestIfLocked(from, receivedProtocol, filename)) {
			return;
		}

		// lock
		lockFile(filename);
		
		// address of node w/ rw or null
		Integer rw = checkRWClients(filename);
		List<Integer> ro = checkROClients(filename);
		
		if (rw == null && ro.size() == 0){
			this.node.sendError(from, Protocol.ERROR, filename,
					ErrorCode.FileDoesNotExist);
		}

		if (rw != null && ro.size() > 0) {
			this.node.sendError(from, filename, "simultaneous RW (" + rw + ") and ROs (" + ro.toString()
					+ ") detected on file: " + filename);
		}

		if (rw != null) { // someone has RW
			// Get updates
			this.node.sendRequest(rw, filename, forwardingProtocol);
			pendingCCPermissionRequests.put(filename, from);
		} else if (!preserveROs && ro.size() > 0) { // someone(s) have RO
			pendingICs.put(filename, ro);
			for (int i : ro) {
				// Invalidate all ROs
				this.node.sendRequest(i, filename, Protocol.IV);
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
				this.node.sendError(from, filename, e.getMessage());
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
		if (!pendingICs.containsKey(filename) || 
				!pendingICs.get(filename).contains(from)) {
			this.node.sendError(from, Protocol.ERROR, filename, ErrorCode.UnknownError);
		} else {

			// update the status of the client who sent the IC
			List<Integer> ro = checkROClients(filename);
			ro.remove(filename);
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
					destAddr = pendingCCPermissionRequests
							.remove(filename);
					try {
						sendFile(destAddr, filename, Protocol.WD);
					} catch (IOException e) {
						this.node.sendError(from, filename, e.getMessage());
					}
				} else {
					destAddr = pendingRPCDeleteRequests.remove(filename);
					this.node.sendSuccess(destAddr, Protocol.DELETE, filename);
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
		if (checkRWClients(filename) == from)
			cacheRW.put(filename, -1);
		
		// check if someone's in the middle of a transaction with this file. if so, don't do anything.
		if (!transactionLockedFiles.containsKey(filename))
			unlockFile(filename);
		
	}

	public void receiveWC(int from, String filename){

		// TODO: Check if someone has RW already, throw error if so
		cacheRW.put(filename, from);
		
		// check if someone's in the middle of a transaction with this file. if so, don't do anything.
		if (!transactionLockedFiles.containsKey(filename))
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
			this.node.sendError(to, Protocol.ERROR, filename, ErrorCode.FileDoesNotExist);
		} else {
			sendMsg.append(filename);
			sendMsg.append(Client.delimiter);
			sendMsg.append(this.node.fs.getFile(filename));
		}

		byte[] payload = Utility.stringToByteArray(sendMsg.toString());
		this.node.printVerbose("sending " + Protocol.protocolToString(protocol) + " to "
				+ to);
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
}
