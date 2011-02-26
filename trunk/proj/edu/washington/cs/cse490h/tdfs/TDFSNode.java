package edu.washington.cs.cse490h.tdfs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

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

	enum NodeTypes {
		Acceptor, Proposer
	}

	// What type of node this PaxosNode is
	protected NodeTypes nodeType;

	// The last proposal number this paxos node has sent. Assumed to start at 0
	// and increment from there.
	private int lastProposalNumberSent;

	// The list of possible values for a proposer to choose. If it's empty, the
	// proposer assumes it can choose anything (but chooses 1 for simplicity).
	private Set<Integer> possibleValues;

	private int proposersResponded;

	// The value the proposer has decided on for a given operation number
	private Map<Integer, Integer> chosenValues;

	/**
	 * A list of known paxos group members for a given filename
	 */
	private Map<String, List<Integer>> knownGroupMembers;

	// The largest proposal number this paxos node has accepted. Assumed to
	// start at -1.
	private int largestProposalNumberAccepted;

	/**
	 * The last value this node accepted. Assumed to be -1 if it has not
	 * accepted any values for this instance.
	 */
	private int lastValueAccepted;

	@Override
	public void start() {
		queuedOperations = new LinkedList<Operation>();
		filesBeingOperatedOn = new ArrayList<String>();
		this.nodeType = NodeTypes.Acceptor; // By default, a node is assumed to
											// be an acceptor
		this.lastProposalNumberSent = -1;
		this.largestProposalNumberAccepted = -1;
		this.lastValueAccepted = -1;
		this.possibleValues = new HashSet<Integer>();
		this.proposersResponded = 0;
		this.chosenValues = new HashMap<Integer, Integer>();
	}

	@Override
	public void onCommand(String command) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onRIOReceive(Integer from, MessageType protocol, byte[] msg) {
		// TODO Auto-generated method stub

	}

	/**
	 * Functionality varies depending on recipient type.
	 * 
	 * ACCEPTOR The acceptor validates this value, if an error hasn't occurred.
	 * Sends a message to the learner, which is also the proposer.
	 * 
	 * PROPOSER/LEARNER The proposer informs all acceptors that a new value has
	 * been chosen, and that this paxos session is finished. After the timeout
	 * elapses, the proposer should initiate a new leader selection, though if
	 * not one of the acceptors will.
	 * 
	 * @from The proposer
	 * @proposalNumber The proposal number, used for validation
	 * @value The chosen value
	 */
	public void receiveAccepted(int from, int proposalNumber, int value) {

		if (nodeType.equals(NodeTypes.Acceptor)) {

			if (proposalNumber < largestProposalNumberAccepted) {
				// TODO: High - Throw an error!
			}

			lastValueAccepted = value;

			RIOSend(from,
					MessageType.Accepted,
					Utility.stringToByteArray(lastProposalNumberSent + " "
							+ chosenValues.get(lastProposalNumberSent)));
		} else {
			receiveFinished(addr);
		}
	}

	/**
	 * An indication from the learner (who is also the proposer) that this
	 * session of paxos is finalized, and that the proposed action has been
	 * accepted.
	 * 
	 * @param from
	 *            The proposer/learner
	 */
	public void receiveFinished(int from) {

	}

	/**
	 * Select a proposal number and send it to each acceptor. Don't need to
	 * worry about a quorum yet - if not enough acceptors are on, won't proceed
	 * past the accept stage and will stall, which is allowable.
	 */
	public void prepare(String filename) {

		if (!nodeType.equals(NodeTypes.Proposer)) {
			// TODO: High - throw an error!
			return;
		}

		lastProposalNumberSent++;

		Iterator<Integer> iter = knownGroupMembers.get(filename).iterator();
		while (iter.hasNext()) {
			int next = iter.next();
			if (next != addr)
				RIOSend(iter.next(), MessageType.Prepare,
						Utility.stringToByteArray(lastProposalNumberSent + ""));
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
	public void receivePromise(int from, int proposalNumber) {
		if (!nodeType.equals(NodeTypes.Acceptor)) {
			return;
		}

		if (proposalNumber < largestProposalNumberAccepted) {
			RIOSend(from,
					MessageType.PromiseDenial,
					Utility.stringToByteArray(largestProposalNumberAccepted
							+ ""));
		}

		else { // accept it and update
			largestProposalNumberAccepted = proposalNumber;
			RIOSend(from, MessageType.Promise,
					Utility.stringToByteArray(lastValueAccepted + ""));
		}
	}

	/**
	 * The proposer receives responses from acceptors, and decides whether to
	 * proceed to the accept sending based upon whether it receives responses
	 * from a quorum. It will do nothing until it receives a quorum - this is
	 * acceptable behavior.
	 * 
	 * The proposer then chooses a value - if the acceptors haven't chosen a
	 * value previously, it just makes one up (1, no need for non-deterministic
	 * behavior).
	 * 
	 * @from The acceptor who sent this message
	 * @lastValueChosen The last value chosen by this acceptor. -1 if the
	 *                  acceptor has never chosen a value.
	 */
	public void receiveAccept(int from, int lastValueChosen, String filename) {

		int chosenValue;

		if (lastValueChosen != -1) {
			possibleValues.add(lastValueChosen);
		}

		proposersResponded++;

		if (proposersResponded < (knownGroupMembers.get(filename).size() / 2))
			return;

		// A majority has responded, so continue on
		if (possibleValues.size() != 0) { // Just choose the first one in the
											// set, no need to decide
			chosenValue = (Integer) possibleValues.toArray()[0];
		} else
			chosenValue = 1;

		Iterator<Integer> iter = knownGroupMembers.get(filename).iterator();
		while (iter.hasNext()) {
			int next = iter.next();
			if (next != addr) {
				RIOSend(iter.next(),
						MessageType.Accept,
						Utility.stringToByteArray(lastProposalNumberSent + " "
								+ chosenValue));
			}
		}

	}
}
