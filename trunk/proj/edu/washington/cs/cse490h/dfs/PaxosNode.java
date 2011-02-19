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

	public static final int leaseTimeout = 50;
	
	enum NodeTypes {
		Acceptor, Proposer
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

	// TODO: High: This may be redundant with the above
	private Set<Integer> knownManagers;
	
	
	// The list of possible values for a proposer to choose. If it's empty, the
	// proposer assumes it can choose anything (but chooses 1 for simplicity).
	private Set<Integer> possibleValues;
	
	private int proposersResponded;

	
	// The value the proposer has decided on
	private int chosenValue;

	// Acceptor/Learner data structures

	// The largest proposal number this paxos node has accepted. Assumed to
	// start at -1.
	private int largestProposalNumberAccepted;

	// The last value this node accepted. Assumed to be -1 if it has not
	// accepted any values for this instance.
	private int lastValueAccepted;
	
	private DFSNode node;

	public PaxosNode(DFSNode n, Set<Integer> managers) {
		this.node = n;
		this.nodeType = NodeTypes.Acceptor; // By default, a node is assumed to
											// be an acceptor
		this.knownAcceptors = managers;
		this.knownManagers = managers;
		this.lastProposalNumberSent = -1;
		this.largestProposalNumberAccepted = -1;
		this.lastValueAccepted = -1;
		this.possibleValues = new HashSet<Integer>();
		this.proposersResponded = 0;
	}

	/**
	 * Each node assumes they are the leader node and sends this message to the list of managers.
	 */
	public void leaderVote(){
		this.nodeType = NodeTypes.Proposer;
		Iterator<Integer> i = knownManagers.iterator();
		while (i.hasNext()){
			this.node.RIOSend(i.next(), MessageType.Leader);
		}
	}
	
	/**
	 * The node just quickly checks if the node they received this message from has a lower address. Otherwise, it sends out 
	 * its own address just in case it didn't get the indication to start a leader election.
	 * @param from The sender
	 */
	public void leaderReceive(int from){
		if (from < this.node.addr){
			this.nodeType = NodeTypes.Acceptor;
		} else
			leaderVote();
		
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
	public void receivePromise(int from, int proposalNumber) {
		if (!nodeType.equals(NodeTypes.Acceptor)) {
			// TODO: throw an error!
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
	public void receiveAccept(int from, int lastValueChosen) {

		if (lastValueChosen != -1) {
			possibleValues.add(lastValueChosen);
		}

		proposersResponded++;

		if (proposersResponded < (knownAcceptors.size() / 2))
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
	 * If the acceptor receives an accept message for a proposal it has promised
	 * to accept, then it accepts the value.
	 * 
	 * Each acceptor then sends an accepted message to the proposer, and every learner.
	 * 
	 */
	/**
	 * The acceptor validates this value, if an error hasn't occurred. Sends a
	 * message to the proposer and learner(s).
	 * 
	 * @from The proposer
	 * @proposalNumber The proposal number, used for validation
	 * @value The chosen value
	 */
	public void receiveAccepted(int from, int proposalNumber, int value) {
		
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
	}
}
