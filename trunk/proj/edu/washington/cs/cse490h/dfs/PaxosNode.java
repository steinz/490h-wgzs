package edu.washington.cs.cse490h.dfs;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import edu.washington.cs.cse490h.lib.Utility;

/**
 * Checks to see if the given proposal number is larger than any previous
 * proposal. Promises to not accept proposals less than the given proposal
 * number if so, and sends the last value it accepted to the proposer (assumed
 * to be who this message is from).
 * 
 * @from Assumed to be the proposer's address
 * @proposalNumber The proposal number this node is proposing
 */

public class PaxosNode {

	enum NodeTypes {
		Learner, Acceptor, Proposer
	}

	// What type of node this PaxosNode is
	protected NodeTypes nodeType;

	// Proposer data structures

	// The last proposal number this paxos node has sent. Assumed to start at 0
	// and increment from there.
	private int lastProposalNumberSent;

	// The list of known acceptors. NOTE: Learners are assumed to be acceptors
	// as well.
	private Set<Integer> knownAcceptors;

	// The list of possible values for a proposer to choose. If it's empty, the
	// proposer assumes it can choose anything (but chooses 1 for simplicity).
	private Set<Integer> possibleValues;

	// The list of proposers who have responded.
	// TODO: High - is this necessary? A simple count could suffice, but...
	private Set<Integer> proposersResponded;

	// The value the proposer has decided on
	private int chosenValue;

	// Acceptor/Learner data structures

	// The largest proposal number this paxos node has accepted. Assumed to
	// start at -1.
	private int largestProposalNumberAccepted;

	// The last value this node accepted. Assumed to be -1 if it has not
	// accepted any values for this instance.
	private int lastValueAccepted;

	// For now, assumed there is a distinguished learner
	private int learnerAddress;

	private DFSNode node;

	public PaxosNode(DFSNode n) {
		this.node = n;
		this.nodeType = NodeTypes.Acceptor; // By default, a node is assumed to
											// be an acceptor
		this.knownAcceptors = new HashSet<Integer>();
		this.lastProposalNumberSent = -1;
		this.largestProposalNumberAccepted = -1;
		this.lastValueAccepted = -1;
		this.possibleValues = new HashSet<Integer>();
		this.proposersResponded = new HashSet<Integer>();
	}

	/**
	 * Select a proposal number and send it to each acceptor. Don't need to
	 * worry about a quorum yet - if not enough acceptors are on, won't proceed
	 * past the accept stage and will stall, which is allowable.
	 */
	public void prepare() {

		if (!nodeType.equals(NodeTypes.Proposer)) {
			// TODO: High - throw an error!
			return;
		}

		lastProposalNumberSent++;

		Iterator<Integer> iter = knownAcceptors.iterator();
		while (iter.hasNext()) {
			this.node.RIOSend(iter.next(), MessageType.Prepare,
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
	public void promise(int from, int proposalNumber) {
		if (!nodeType.equals(NodeTypes.Acceptor)
				&& !nodeType.equals(NodeTypes.Learner)) {
			// TODO: High - throw an error!
			return;
		}

		if (proposalNumber < largestProposalNumberAccepted) {
			this.node.RIOSend(
					from,
					MessageType.PromiseDenial,
					Utility.stringToByteArray(largestProposalNumberAccepted
							+ ""));
		}

		else { // accept it and update
			largestProposalNumberAccepted = proposalNumber;
			this.node.RIOSend(from, MessageType.Promise,
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
	public void accept(int from, int lastValueChosen) {

		if (lastValueChosen != -1) {
			possibleValues.add(lastValueChosen);
		}

		proposersResponded.add(from);

		if (proposersResponded.size() < (knownAcceptors.size() / 2))
			return;

		// A majority has responded, so continue on
		if (possibleValues.size() != 0) { // Just choose the first one in the
											// set, no need to decide
			chosenValue = (Integer) possibleValues.toArray()[0];
		} else
			chosenValue = 1;

		Iterator<Integer> iter = knownAcceptors.iterator();
		while (iter.hasNext()) {
			this.node.RIOSend(
					iter.next(),
					MessageType.Accept,
					Utility.stringToByteArray(lastProposalNumberSent + " "
							+ chosenValue));
		}

	}

	/*
	 * If the Acceptor receives an Accept! message for a proposal it has not
	 * promised not to accept in 1b, then it Accepts the value.
	 * 
	 * Each Acceptor sends an Accepted message to the Proposer and every
	 * Learner.
	 */
	/**
	 * The acceptor validates this value, if an error hasn't occurred. Sends a
	 * message to the proposer and learner(s).
	 * 
	 * @from The proposer
	 * @proposalNumber The proposal number, used for validation
	 * @value The chosen value
	 */
	public void accepted(int from, int proposalNumber, int value) {

		// Did we even promise this proposal number?
		if (proposalNumber < largestProposalNumberAccepted) {
			// TODO: High - Throw an error!
		}

		lastValueAccepted = value;

		this.node.RIOSend(
				from,
				MessageType.Accepted,
				Utility.stringToByteArray(lastProposalNumberSent + " "
						+ chosenValue));
		this.node.RIOSend(
				learnerAddress,
				MessageType.Accepted,
				Utility.stringToByteArray(lastProposalNumberSent + " "
						+ chosenValue));
	}
}
