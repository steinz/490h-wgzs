package edu.washington.cs.cse490h.tdfs;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.StringTokenizer;
import edu.washington.cs.cse490h.lib.Utility;

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

	
	/**
	 *  PAXOS Structures
	 */
	
	/**
	 * Learner only: Number of acceptors that have contacted the learner about accepting a particular N,V pair
	 */
	private int acceptorsResponded;
	
	/**
	 * Proposer only: Number of acceptors that have responded with a promise
	 */
	private int promisesReceived;

	/**
	 * Operation number, Operation pairs for Paxos
	 */
	private Map<Integer, Operation> chosenValues;

	/**
	 * A list of known paxos group members for a given filename
	 */
	private Map<String, List<Integer>> knownGroupMembers;
	
	/**
	 * A list of known learners for a given filename
	 */
	private Map<String, List<Integer>> knownLearners;

	/**
	 * The largest proposal number this node has accepted
	 */
	private int largestProposalNumberAccepted;

	

	@Override
	public void start() {
		queuedOperations = new LinkedList<Operation>();
		filesBeingOperatedOn = new ArrayList<String>();
		
		// Paxos
		this.largestProposalNumberAccepted = -1;
		this.acceptorsResponded = 0;
		this.chosenValues = new HashMap<Integer, Operation>();
	}

	@Override
	public void onCommand(String line) {
		// TODO: fix or something

		// Create a tokenizer and get the first token (the actual cmd)
		StringTokenizer tokens = new StringTokenizer(line, " ");
		String cmd = "";
		try {
			cmd = tokens.nextToken().toLowerCase();
		} catch (NoSuchElementException e) {
			// TODO: parent.printError("no command found in: " + line);
			return;
		}

		/*
		 * Dynamically call <cmd>Command, passing off the tokenizer and the full
		 * command string
		 */
		try {
			Class<?>[] paramTypes = { StringTokenizer.class, String.class };
			Method handler = this.getClass().getMethod(cmd + "Handler",
					paramTypes);
			Object[] args = { tokens, line };
			handler.invoke(this, args);
		} catch (NoSuchMethodException e) {
			// TODO: parent.printError("invalid command:" + line);
		} catch (IllegalAccessException e) {
			// TODO: parent.printError("invalid command:" + line);
		} catch (InvocationTargetException e) {
			// TODO: parent.printError(e.getCause());
		}

	}

	@Override
	public void onRIOReceive(Integer from, MessageType type, byte[] msg) {
		String msgString = Utility.byteArrayToString(msg);

		// TDFS handles all non-RIO messages right now
		Object instance = this;

		// route message
		try {
			Class<?> handlingClass = instance.getClass();
			Class<?>[] paramTypes = { int.class, String.class };
			Method handler = handlingClass.getMethod("receive" + type.name(),
					paramTypes);
			Object[] args = { from, msgString };
			handler.invoke(instance, args);
		} catch (Exception e) {
			printError(e);
		}
	}

	/**
	 * Functionality varies depending on recipient type.
	 * 
	 * The acceptor validates this value, if an error hasn't occurred.
	 * Sends a message to the learners
	 * 
	 * @op The operation
	 */
	public void receiveValue(Operation op) {
			
		// STUB
		String filename = "";
		// END STUB
		
		Iterator<Integer> iter = knownLearners.get(filename).iterator();
		while (iter.hasNext()){
			int next = iter.next();
			if (next != addr)
				RIOSend(next, MessageType.Accepted, op.pack()); 
			// TODO: High: It needs to send the filename, proposal number, and operation all together. 
			// It would be nice if the operation encapsulated all of those things.
		}
	}

	/**
	 * The learner waits to hear from a majority of acceptors. If it has, it sends out a message to all paxos
	 * nodes that this value has been chosen and writes it to its own local log.
	 * 
	 * @param op The operation
	 */
	public void receiveAccepted(Operation op) {
		// STUB
		int proposalNumber = 0; // TODO: High - these should be part of the operation, or passed somehow, but I'm not sure how.
		String filename = "";
		
		// END STUB
		
		acceptorsResponded++;
		if (acceptorsResponded < knownGroupMembers.get(filename).size() / 2)
			return;
		
		Iterator<Integer> iter = knownGroupMembers.get(filename).iterator();
		while (iter.hasNext()){
			int next = iter.next();
			if (next != addr)
				RIOSend(next, MessageType.Finished, op.pack());
		}
		
		// Put this operation into the chosen values map
		chosenValues.put(proposalNumber, op);
		// TODO: High - write to local log
		
	}

	/**
	 * Select a proposal number and send it to each acceptor. Don't need to
	 * worry about a quorum yet - if not enough acceptors are on, won't proceed
	 * past the accept stage and will stall, which is allowable.
	 */
	public void prepare(Operation op) {
		
		// STUB
		Integer proposalNumber = null;
		String filename = "";
		// END STUB
		
		// TODO: High - the proposal number and filename should probably be part of the operation
		int prepareProposalNumber = -1;
		if (proposalNumber == null) {
			prepareProposalNumber = ++largestProposalNumberAccepted;
		} else
			prepareProposalNumber = proposalNumber;

		Iterator<Integer> iter = knownGroupMembers.get(filename).iterator();
		while (iter.hasNext()) {
			int next = iter.next();
			if (next != addr)
				RIOSend(iter.next(), MessageType.Prepare,
						Utility.stringToByteArray(prepareProposalNumber + ""));
		}

	}

	/**
	 * Checks to see if the given proposal number is larger than any previous
	 * proposal. Promises to not accept proposals less than the given proposal
	 * number if so, and sends the last value it accepted to the proposer
	 * (assumed to be who this message is from).
	 * 
	 * @from Assumed to be the proposer's address
	 * @proposalNumber The proposal number this node is proposing
	 */
	public void receivePrepare(int from, int proposalNumber) {

		if (proposalNumber <= largestProposalNumberAccepted) {
			RIOSend(from,
					MessageType.PromiseDenial,
					chosenValues.get(proposalNumber).pack());
		}

		else { // accept it and update
			largestProposalNumberAccepted = proposalNumber;
			RIOSend(from, MessageType.Promise,
					Utility.stringToByteArray(proposalNumber + ""));
		}
	}

	/**
	 * The proposer receives responses from acceptors, and decides whether to
	 * proceed to the accept sending based upon whether it receives responses
	 * from a quorum. It will do nothing until it receives a quorum - this is
	 * acceptable behavior.
	 * 
	 * 
	 * @from The acceptor who sent this message
	 * @lastValueChosen The last value chosen by this acceptor. -1 if the
	 *                  acceptor has never chosen a value.
	 */
	public void receivePromise(Operation op) {

		// STUB
		String filename = "";
		// END STUB

		promisesReceived++;

		if (promisesReceived < (knownGroupMembers.get(filename).size() / 2))
			return;


		Iterator<Integer> iter = knownGroupMembers.get(filename).iterator();
		while (iter.hasNext()) {
			int next = iter.next();
			if (next != addr) {
				RIOSend(iter.next(),
						MessageType.Accept,
						op.pack());
			}
		}

	}
}
