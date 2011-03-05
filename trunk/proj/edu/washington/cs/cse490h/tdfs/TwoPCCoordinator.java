package edu.washington.cs.cse490h.tdfs;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.washington.cs.cse490h.lib.Callback;

public class TwoPCCoordinator extends RIONode {

	private final int TXTIMEOUT = 20;
	private LogFS logFs;
	private Map<Integer, List<String>> clientTransactionFiles;
	private int coordinatorCount;
	private Map<String, Integer> lastProposalNumbersSent;

	@Override
	public void start() {
		this.logFs = new LogFileSystem();
		clientTransactionFiles = new HashMap<Integer, List<String>>();
	}

	public void setCoordinatorCount(int count) {
		this.coordinatorCount = count;
	}

	@Override
	public void onRIOReceive(Integer from, MessageType protocol, byte[] msg) {
		if (!protocol.equals(MessageType.Learned)) {
			Logger.error(this, "TwoPCCoordinator received non-learn message: "
					+ protocol.toString());
		}
		learn(from, msg);
	}

	public void learn(int from, byte[] msg) {
		Proposal p = new Proposal(msg);
		logFs.writeLogEntry(p.filename, p.operationNumber, p.operation);

		// check for transactions

		if (p.operation instanceof TXStartLogEntry) {
			// start callback
			addTxTimeout(from);
			// add files
			String[] files = ((TXStartLogEntry) p.operation).filenames;
			if (!clientTransactionFiles.get(from).isEmpty()) { // client didn't abort or commit last tx
				Logger.error(this, "Client: " + from
						+ " did not commit or abort last tx!");
			}
			for (String file : files) {
				clientTransactionFiles.get(from).add(file);
			}

		}

		else if (p.operation instanceof TXTryCommitLogEntry) {
			// commit to each filename coordinator
			String[] files = ((TXTryCommitLogEntry) p.operation).filenames;
			createProposal(new TXTryCommitLogEntry(files), files);
			clientTransactionFiles.get(from).clear();
		}

		else if (p.operation instanceof TXTryAbortLogEntry) {
			String[] files = ((TXTryAbortLogEntry) p.operation).filenames;
			createProposal(new TXTryAbortLogEntry(files), files);
			clientTransactionFiles.get(from).clear();
		}
	}
	
	/**
	 * Creates a proposal using the appropriate log entry type,
	 * @param operation
	 * @param files
	 */
	public void createProposal(LogEntry operation, String[] files){
		for (String file : files) {
			List<Integer> coordinators = TDFSNode.getParticipants(file);
			Proposal newProposal = new Proposal(
					new TXCommitLogEntry(files), file,
					logFs.nextLogNumber(file), nextProposalNumber(file));

			for (Integer addr : coordinators){
				RIOSend(addr, MessageType.Prepare, newProposal.pack());
			}
		}
	}

	/**
	 * Same function as TDFSNode.nextProposalNumber. The only reason this is copy/pasted
	 * is that the lastProposalNumbersSent data structure is highly specific to each node.
	 * @param filename The filename 
	 * @return The next proposal number
	 * @throws NotListeningException
	 */
	public int nextProposalNumber(String filename) throws NotListeningException {
		List<Integer> participants = TDFSNode.getParticipants(filename);
		Integer lastNumSent = lastProposalNumbersSent.get(filename);
		if (lastNumSent == null) {
			// use offset
			Collections.sort(participants); // TODO: document that this is ok
			int number = participants.indexOf(addr);
			lastProposalNumbersSent.put(filename, number);
			return number;
		} else {
			// increment last sent by participation count
			int number = lastNumSent + participants.size();
			lastProposalNumbersSent.put(filename, number);
			return number;
		}

	}

	public void abortClient(int client) {

	}

	/**
	 * Adds a transaction timeout for the given client. If the client hasn't
	 * committed their transaction by the time the lease expires, then
	 * 
	 * @param client
	 */
	public void addTxTimeout(int client) {
		Method cbMethod = null;
		try {
			String[] params = { "java.lang.Integer" };
			cbMethod = Callback.getMethod("abortClient", this, params);
			cbMethod.setAccessible(true); // HACK
		} catch (Exception e) {
			printError(e);
			e.printStackTrace();
		}
		Integer[] args = { client };
		Callback cb = new Callback(cbMethod, this, args);
		addTimeout(cb, this.TXTIMEOUT);
	}

	@Override
	public void onCommand(String command) {
		// TODO Auto-generated method stub

	}

}
