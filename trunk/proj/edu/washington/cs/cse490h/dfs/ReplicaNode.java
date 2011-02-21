package edu.washington.cs.cse490h.dfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/*
 * TODO: HIGH: Replicas replicating the same node can become out of sync w/ eachother
 * 
 * Consider:
 * 
 * ...
 * 
 * 1 txcommit
 * 
 * 1 replicateCommit()
 * 
 * 1 commitCurrentTransaction()
 * 
 * 2 receiveReplicaTXCommit()
 * 
 * 1 crashes
 * 
 * 3 receiveReplicaTXCommit() <- never happens, 2 and 3 are out of sync re: most recent tx
 * 
 * When is this a problem... how do we fix it?
 * 
 * Maybe whichever replica services a request has to update the other replicas... messy...
 * they could use PAXOS to reach a concensus... lol
 */

public class ReplicaNode {

	/**
	 * The parent node associated with this relica
	 */
	private DFSNode parent;

	/**
	 * Nodes this node is replicating
	 */
	private List<Integer> replicating;

	/**
	 * Subset of replicating who are currently performing a tx
	 */
	private Set<Integer> replicatingInTX;

	public ReplicaNode(DFSNode n) {
		this.parent = n;

		this.replicating = new ArrayList<Integer>();
		this.replicatingInTX = new HashSet<Integer>();
	}

	/*
	 * TODO: HIGH: All of these should just throw runtime exceptions
	 */

	public void receiveReplicaAppend(int from, String msg)
			throws FileNotFoundException, TransactionException, IOException {
		receiveReplicaWrite(from, msg, true);
	}

	public void receiveReplicaCreate(int from, String filename)
			throws FileAlreadyExistsException, IOException,
			TransactionException {
		if (replicatingInTX.contains(from)) {
			parent.fs.createFileTX(from, filename);
		} else {
			parent.fs.createFile(filename);
		}
	}

	public void receiveReplicaDelete(int from, String filename)
			throws FileNotFoundException, TransactionException, IOException {
		if (replicatingInTX.contains(from)) {
			parent.fs.deleteFileTX(from, filename);
		} else {
			parent.fs.deleteFile(filename);
		}
	}

	public void receiveReplicaPut(int from, String msg)
			throws FileNotFoundException, TransactionException, IOException {
		receiveReplicaWrite(from, msg, false);
	}

	public void receiveReplicaTXAbort(int from, String msg) throws IOException {
		parent.fs.abortTransaction(from);
	}

	public void receiveReplicaTXCommit(int from, String msg) throws IOException {
		parent.fs.commitTransaction(from);
	}

	public void receiveReplicaTXStart(int from, String msg) throws IOException {
		parent.fs.startTransaction(from);
	}

	public void receiveReplicaStartReplicating(int from, String address) {
		int a = Integer.parseInt(address);
		replicating.add(a);
	}

	private void receiveReplicaWrite(int from, String msg, boolean append)
			throws FileNotFoundException, TransactionException, IOException {
		int delimIndex = msg.indexOf(DFSNode.packetDelimiter);
		String filename = msg.substring(0, delimIndex);
		String contents = msg.substring(delimIndex
				+ DFSNode.packetDelimiter.length());

		if (replicatingInTX.contains(from)) {
			if (!parent.fs.fileExistsTX(from, filename)) {
				parent.fs.createFileTX(from, filename);
			}
			parent.fs.writeFileTX(from, filename, contents, true);
		} else {
			parent.fs.writeFile(filename, contents, append);
		}
	}
}
