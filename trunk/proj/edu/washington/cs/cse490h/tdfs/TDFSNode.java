package edu.washington.cs.cse490h.tdfs;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
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
	 * PAXOS Structures
	 */

	/**
	 * Learner only: Number of acceptors that have contacted the learner about
	 * accepting a particular N,V pair
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
	 * The largest proposal number this node has accepted
	 */
	private int largestProposalNumberAccepted;

	private LogFileSystem logFS;

	@Override
	public void start() {
		queuedOperations = new LinkedList<Operation>();
		filesBeingOperatedOn = new ArrayList<String>();
		this.logFS = new LogFileSystem();

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
	 * A lead proposer receives a proposal from a node. The lead proposer checks
	 * if this node is part of the paxos group, and if it's not it checks
	 * whether this proposal is a join. If it is not a join and the node is not
	 * part of the paxos group, then the proposal is rejected.
	 * 
	 * @param prop
	 *            The proposal encapsulated
	 */
	public void receiveRequest(int from, byte[] msg) {

		Proposal proposal = new Proposal(msg);

		List<Integer> participants = null;
		try {
			participants = logFS.getParticipants(proposal.filename);
		} catch (NotParticipatingException e) {
			// TODO: Deal with exception
		}
		if (!participants.contains(from)
				&& !(proposal.operation instanceof Join)) {
			// TODO: High: Send an error back
			return;
		}

		Iterator<Integer> iter = participants.iterator();
		while (iter.hasNext()) {
			int next = iter.next();
			if (next != addr)
				RIOSend(next, MessageType.Prepare, msg);
		}
	}

	/**
	 * Functionality varies depending on recipient type.
	 * 
	 * The acceptor validates this value, if an error hasn't occurred. Sends a
	 * message to the learners
	 * 
	 * @op The operation
	 */
	public void receiveValue(int from, byte[] msg) {
		RIOSend(from, MessageType.Accepted, msg);

	}

	/**
	 * The learner waits to hear from a majority of acceptors. If it has, it
	 * sends out a message to all paxos nodes that this value has been chosen
	 * and writes it to its own local log.
	 * 
	 * @param op
	 *            The operation
	 */
	public void receiveAccepted(int from, byte[] msg) {
		Proposal proposal = new Proposal(msg);
		String filename = proposal.filename;
		int proposalNumber = proposal.proposalNumber;
		Operation op = proposal.operation;

		List<Integer> participants = null;
		try {
			participants = logFS.getParticipants(filename);
		} catch (NotParticipatingException e) {
			// TODO: High: Deal with exception
		}

		acceptorsResponded++;
		if (acceptorsResponded < participants.size() / 2)
			return;

		Iterator<Integer> iter = participants.iterator();
		while (iter.hasNext()) {
			int next = iter.next();
			if (next != addr)
				RIOSend(next, MessageType.Learned, msg);
		}

		// Put this operation into the chosen values map
		chosenValues.put(proposalNumber, op);
		// TODO: High - write to local log

		RIOSend(from, MessageType.Joined, Utility.stringToByteArray(filename));

		if (op instanceof Join)
			try {
				logFS.join(filename, from);
			} catch (NotParticipatingException e) {
				// TODO: High: Send error back
			}

	}

	/**
	 * Select a proposal number and send it to each acceptor. Don't need to
	 * worry about a quorum yet - if not enough acceptors are on, won't proceed
	 * past the accept stage and will stall, which is allowable.
	 */
	public void prepare(byte[] msg) {

		Proposal proposal = new Proposal(msg);
		int proposalNumber = proposal.proposalNumber;
		String filename = proposal.filename;
		int prepareNumber;

		if (proposalNumber == -1) {
			prepareNumber = ++largestProposalNumberAccepted;
		} else
			prepareNumber = proposalNumber;

		try {
			Iterator<Integer> iter = logFS.getParticipants(filename).iterator();
			while (iter.hasNext()) {
				int next = iter.next();
				if (next != addr)
					RIOSend(iter.next(), MessageType.Prepare,
							Utility.stringToByteArray(prepareNumber + ""));
			}

		} catch (NotParticipatingException e) {
			// TODO: High: Send this error back
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
	public void receivePrepare(int from, byte[] msg) {

		Proposal proposal = new Proposal(msg);
		int proposalNumber = proposal.proposalNumber;
		int operationNumber = proposal.operationNumber;
		String filename = proposal.filename;

		if (proposalNumber <= largestProposalNumberAccepted) {
			RIOSend(from, MessageType.PromiseDenial,
					chosenValues.get(proposalNumber).pack());
			return;
		}

		try {
			if (logFS.getNextOperationNumber(filename) >= operationNumber) {
				RIOSend(from, MessageType.OldOperation,
						logFS.getOperation(filename, operationNumber).pack());

				// TODO High: Don't force them to resend every operation number,
				// somehow update them to latest version
				// Check if forgotten operation
				return;
			}
		} catch (NotParticipatingException e) {
			// TODO High: Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchOperationNumber e) {
			// TODO High: Auto-generated catch block
			e.printStackTrace();
		}

		largestProposalNumberAccepted = proposalNumber;
		RIOSend(from, MessageType.Promise,
				Utility.stringToByteArray(proposalNumber + ""));

	}

	/**
	 * The proposer receives responses from acceptors, and decides whether to
	 * proceed to the accept sending based upon whether it receives responses
	 * from a quorum. It will do nothing until it receives a quorum - this is
	 * acceptable behavior.
	 * 
	 */
	public void receivePromise(byte[] msg) {

		Proposal proposal = new Proposal(msg);
		String filename = proposal.filename;
		List<Integer> participants = null;

		try {
			participants = logFS.getParticipants(filename);
		} catch (NotParticipatingException e) {
			// TODO: High: Send exception back
		}

		promisesReceived++;

		if (promisesReceived < (participants.size() / 2))
			return;

		Iterator<Integer> iter = participants.iterator();
		while (iter.hasNext()) {
			int next = iter.next();
			if (next != addr) {
				RIOSend(iter.next(), MessageType.Accept, msg);
			}
		}

	}

	/**
	 * This proposal is now learned, so put it in the log
	 * 
	 * @param msg
	 *            The proposal, as a byte array
	 */
	public void receiveLearned(byte[] msg) {
		// TODO: High: Add to log
	}
}
