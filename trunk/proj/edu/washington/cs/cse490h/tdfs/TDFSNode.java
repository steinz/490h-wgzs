package edu.washington.cs.cse490h.tdfs;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class TDFSNode extends RIONode {

	/*
	 * TODO: HIGH: Coordinator is lead proposer/learner - if can't reach
	 * coordinator, go to second or third coordinator ((hash + {1,2}) %
	 * nodeCount)
	 * 
	 * Client sends to lead (operationNumber, operation) pairs it wants the
	 * leader to propose
	 */

	Queue<Operation> queuedOperations;

	List<String> filesBeingOperatedOn;

	@Override
	public void start() {
		queuedOperations = new LinkedList<Operation>();
		filesBeingOperatedOn = new ArrayList<String>();
	}

	@Override
	public void onCommand(String command) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onRIOReceive(Integer from, MessageType protocol, byte[] msg) {
		// TODO Auto-generated method stub

	}

}
