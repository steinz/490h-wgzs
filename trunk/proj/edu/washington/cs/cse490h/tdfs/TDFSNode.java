package edu.washington.cs.cse490h.tdfs;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
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
	 * TODO: HIGH:
	 * 
	 * OnCommand Handlers
	 * 
	 * Paxos Flow / Correctness
	 * 
	 * Stable Storage
	 */

	/*
	 * TODO: Support dynamic coordinator groups:
	 * 
	 * Include the coordinator count as part of the filename (not necessarily
	 * literally), and then use a biased hash function so that the expected
	 * distribution of hash codes stays ~uniform even when the number of
	 * coordinators changes: as long as nodes usually know the actual total
	 * number of coordinators, the distribution of hash codes should be good
	 */

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

	private int coordinatorCount;

	private Map<String, Integer> lastProposalNumbersSent;

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
	 * Last proposal number promised for a given file
	 */
	private Map<String, Integer> lastProposalNumberPromised;

	private LogFS logFS;

	@Override
	public void start() {
		queuedOperations = new LinkedList<Operation>();
		filesBeingOperatedOn = new ArrayList<String>();
		this.logFS = new LogFileSystem();
		this.coordinatorCount = 3; // TODO: HIGH: configHandler
		this.lastProposalNumbersSent = new HashMap<String, Integer>();

		// Paxos
		this.acceptorsResponded = 0;
		this.lastProposalNumberPromised = new HashMap<String, Integer>();
	}

	@Override
	public void onCommand(String line) {
		// TODO: fix or something

		// Create a tokenizer and get the first token (the actual cmd)
		StringTokenizer tokens = new StringTokenizer(line, " ");
		String cmd= "";
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
	
	public void txstartHandler(StringTokenizer tokens, String line){
		
		String filename = "";
		try {
			filename = tokens.nextToken().toLowerCase();
		} catch (NoSuchElementException e) {
			// TODO: parent.printError("no filename found in: " + line);
			return;
		}
		if (joinedAlready(filename)){
			int nextOperation = -1;
			try {
				nextOperation = logFS.getNextOperationNumber(filename);
			} catch (NotParticipatingException e) {
				// TODO HIGH: Log error
			}
			Proposal proposal = null;
			try {
				proposal = new Proposal(new TXStart(), filename, nextOperation, nextProposalNumber(filename));
			} catch (NotParticipatingException e) {
				// TODO Log error/throw exception
			}
			prepare(addr, proposal);
		}
		
		
	}
	
	public void txcommitHandler(StringTokenizer tokens, String line){
		
		String filename = "";
		try {
			filename = tokens.nextToken().toLowerCase();
		} catch (NoSuchElementException e) {
			// TODO: parent.printError("no filename found in: " + line);
			return;
		}
		
		if (joinedAlready(filename)){
			int nextOperation = -1;
			try {
				nextOperation = logFS.getNextOperationNumber(filename);
			} catch (NotParticipatingException e) {
				// TODO HIGH: Log error
			}
			Proposal proposal = null;
			try {
				proposal = new Proposal(new TXCommit(), filename, nextOperation, nextProposalNumber(filename));
			} catch (NotParticipatingException e) {
				// TODO Log error/throw exception
				e.printStackTrace();
			}
			prepare(addr, proposal);
		}

	}
	
	public void putHandler(StringTokenizer tokens, String line){
		String filename, contents = "";
		try {
			filename = tokens.nextToken().toLowerCase();
			contents = tokens.nextToken().toLowerCase();
		} catch (NoSuchElementException e) {
			// TODO: parent.printError("no filename or contents found in: " + line);
			return;
		}
		
		if (joinedAlready(filename)){
			int nextOperation = -1;
			try {
				nextOperation = logFS.getNextOperationNumber(filename);
			} catch (NotParticipatingException e) {
				// TODO HIGH: Log error
			}
			Proposal proposal = null;
			try {
				proposal = new Proposal(new Write(contents, false), filename, nextOperation, nextProposalNumber(filename));
			} catch (NotParticipatingException e) {
				// TODO Log error/throw exception
			}
			prepare(addr, proposal);
		}
	}

	public void appendHandler(StringTokenizer tokens, String line){
		String filename, contents = "";
		try {
			filename = tokens.nextToken().toLowerCase();
			contents = tokens.nextToken().toLowerCase();
		} catch (NoSuchElementException e) {
			// TODO: parent.printError("no filename or contents found in: " + line);
			return;
		}
		
		if (joinedAlready(filename)){
			int nextOperation = -1;
			try {
				nextOperation = logFS.getNextOperationNumber(filename);
			} catch (NotParticipatingException e) {
				// TODO Log error
			}
			Proposal proposal = null;
			try {
				proposal = new Proposal(new Write(contents, true), filename, nextOperation, nextProposalNumber(filename));
			} catch (NotParticipatingException e) {
				// TODO Log error/throw exception
			}
			prepare(addr, proposal);
		}
	}
	
	public void getHandler(){
		
	}
	
	public void createHandler(StringTokenizer tokens, String line){
		String filename, contents = "";
		try {
			filename = tokens.nextToken().toLowerCase();
		} catch (NoSuchElementException e) {
			// TODO: parent.printError("no filename or contents found in: " + line);
			return;
		}
		
		if (joinedAlready(filename)){
			int nextOperation = -1;
			try {
				nextOperation = logFS.getNextOperationNumber(filename);
			} catch (NotParticipatingException e) {
				// TODO Log error
			}
			Proposal proposal = null;
			try {
				proposal = new Proposal(new Create(), filename, nextOperation, nextProposalNumber(filename));
			} catch (NotParticipatingException e) {
				// TODO Log error/throw exception
			}
			prepare(addr, proposal);
		}
	}
	
	public void deleteHandler(StringTokenizer tokens, String line){
		String filename, contents = "";
		try {
			filename = tokens.nextToken().toLowerCase();
		} catch (NoSuchElementException e) {
			// TODO: parent.printError("no filename or contents found in: " + line);
			return;
		}
		
		if (joinedAlready(filename)){
			int nextOperation = -1;
			try {
				nextOperation = logFS.getNextOperationNumber(filename);
			} catch (NotParticipatingException e) {
				// TODO HIGH: Log error
			}
			Proposal proposal = null;
			try {
				proposal = new Proposal(new Write(contents, true), filename, nextOperation, nextProposalNumber(filename));
			} catch (NotParticipatingException e) {
				// TODO Log error/throw exception
			}
			prepare(addr, proposal);
		}
	}
	
	
	
	/**
	 * Checks whether a node has joined the paxos group for a file already. If it hasn't, it automatically sends the join request.
	 * @param filename The filename for the paxos group
	 * @return True if the node has joined already, false otherwise
	 */
	public boolean joinedAlready(String filename){
		if (!logFS.isParticipating(filename)){
			// TODO: High: Queue request or something
			Join(filename);
			return false;
		}
		return true;
		
	}
	
	public void Join(String filename){
		Proposal proposal = null;
		try {
			proposal = new Proposal(new Join(addr), filename, -1, nextProposalNumber(filename));
		} catch (NotParticipatingException e1) {
			// TODO Log error/throw exception
		}
		int coordinator = 0; //hashFilename(filename);
		if (coordinator == addr){
			try {
				logFS.createGroup(filename);
			} catch (AlreadyParticipatingException e) {
				// TODO Error: Node already participating in group
			}
		}
		RIOSend(coordinator, MessageType.Request, proposal.pack());
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
	 * Relies on participants being static for any given operation number
	 */
	private int nextProposalNumber(String filename)
			throws NotParticipatingException {
		List<Integer> participants = logFS.getParticipants(filename);
		Integer lastNumSent = lastProposalNumbersSent.get(filename);
		if (lastNumSent == null) {
			// use offset
			Collections.sort(participants); // TODO: document that this is ok
			int number = participants.indexOf(this.addr);
			lastProposalNumbersSent.put(filename, number);
			return number;
		} else {
			// increment last sent by participation count
			int number = lastNumSent + participants.size();
			lastProposalNumbersSent.put(filename, number);
			return number;
		}
	}

	private int hashFilename(String filename) {
		return filename.hashCode() % coordinatorCount;
	}
	
	/**
	 * The proposer checks
	 * if this node is part of the paxos group, and if it's not it checks
	 * whether this proposal is a join. If it is not a join and the node is not
	 * part of the paxos group, then the proposal is rejected.
	 * 
	 * @param prop
	 *            The proposal encapsulated
	 */
	public void receiveRequest(int from, byte[] msg) {

		Proposal proposal = new Proposal(msg);
		prepare(from, proposal);
	}
	
	/**
	 * Sends a prepare request to all acceptors in this Paxos group.
	 * @param proposal The proposal to send
	 */
	public void prepare(int from, Proposal proposal)
	{
		List<Integer> participants = null;
		try {
			participants = logFS.getParticipants(proposal.filename);
		} catch (NotParticipatingException e) {
			// TODO: Deal with exception
		}
		
		if (!participants.contains(from)
				&& !(proposal.operation instanceof Join)) {
			// TODO: High: Log error
			return;
		}
		
		Iterator<Integer> iter = participants.iterator();
		while (iter.hasNext()) {
			int next = iter.next();
			if (next != addr)
				RIOSend(next, MessageType.Prepare, proposal.pack());
		}
	}

	/**
	 * Functionality varies depending on recipient type.
	 * 
	 * The acceptor validates this value, if an error hasn't occurred. Sends a
	 * message to the learner, who is also the proposer
	 * 
	 * @op The operation
	 */
	public void receiveAccept(int from, byte[] msg) {
		RIOSend(from, MessageType.Accepted, msg);
	}

	/**
	 * The learner waits to hear from a majority of acceptors. If it has, it
	 * sends out a message to all paxos nodes that this value has been chosen
	 * and writes it to its own local log.
	 * 
	 * @param from
	 *            The sender
	 * @param msg
	 *            The msg, packed as a byte array
	 */
	public void receiveAccepted(int from, byte[] msg) {
		Proposal proposal = new Proposal(msg);
		String filename = proposal.filename;
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

		// TODO: High - write to local log


		if (op instanceof Join)
			try {
				RIOSend(from, MessageType.Joined, Utility.stringToByteArray(filename));
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
	public void prepare(int from, byte[] msg) {

		Proposal proposal = new Proposal(msg);
		int proposalNumber = proposal.proposalNumber;
		String filename = proposal.filename;
		Integer prepareNumber = null;

		if (proposalNumber == -1) {
			try {
				prepareNumber = nextProposalNumber(filename);
			} catch (NotParticipatingException e) {
				// TODO: Catch error/log
			}
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
			// TODO: High: Log
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
		int largestProposalNumberAccepted = lastProposalNumberPromised.get(filename);
		
		
		if (proposalNumber <= largestProposalNumberAccepted) {
			String lastProposalNumber = lastProposalNumberPromised.get(filename) + "";
			RIOSend(from, MessageType.PromiseDenial, Utility.stringToByteArray(lastProposalNumber));
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
		} catch (NoSuchOperationNumberException e) {
			// TODO High: Auto-generated catch block
			e.printStackTrace();
		}

		largestProposalNumberAccepted = proposalNumber;
		RIOSend(from, MessageType.Promise,
				Utility.stringToByteArray(proposalNumber + ""));

	}
	
	public void sendFile(String filename){
	
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
