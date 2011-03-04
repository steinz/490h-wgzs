package edu.washington.cs.cse490h.tdfs;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import edu.washington.cs.cse490h.lib.Utility;

public class TDFSNode extends RIONode {

	/*
	 * TODO: HIGH:
	 * 
	 * OnCommand Handlers:
	 * 
	 * TXStart(f1); x = Get(f1); Put(f1, "..."); TXCommit(); ->
	 * 
	 * Lock(f1); TXStart(f1); Get(f1); Put(f1, "..."); TXCommit(f1); Unlock(f1);
	 * 
	 * Paxos Flow / Correctness
	 * 
	 * (Req -1>) Prepare -> OldOp -1> (done) | PromiseDenial -1> (done) |
	 * Promise -1> Accept -> AcceptDenial -1> (done) | Accepted -1> Learned ->
	 * (done)
	 * 
	 * Stable Storage
	 * 
	 * Ensure majority checks using > not >= half count
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

	/**
	 * Queues commands on a per filename basis
	 */
	private static class FileCommandExecutor {
		/**
		 * Filename => Queued Commands
		 * 
		 * A filename is "locked" iff it has a queue in this map
		 */
		private Map<String, Queue<Command>> fileQueues = new HashMap<String, Queue<Command>>();

		// TODO: HIGH: add list of commands that have been executed by handle
		// but not
		// finished by next

		public void execute(FileCommand command) {
			Queue<Command> queue = fileQueues.get(command.filename);
			if (queue == null) {
				fileQueues.put(command.filename, new LinkedList<Command>());
				command.execute(null, null);
			} else {
				queue.add(command);
			}
		}

		/**
		 * Should be called after done handling any command
		 * 
		 * If commands have been queued on filename: remove and return the next
		 * command from the queue
		 * 
		 * If no commands are queued on filename: unlock filename by removing
		 * its queue and return null
		 */
		public void next(String filename) {
			Queue<Command> queue = fileQueues.get(filename);
			Command command = queue.poll();
			if (command == null) {
				fileQueues.remove(filename);
			} else {
				command.execute(null, null);
			}
		}

		public boolean executingOn(String filename) {
			return fileQueues.containsKey(filename);
		}

		public boolean empty() {
			return fileQueues.size() == 0;
		}
	}

	private static class TransactionQueue {
		private boolean inTx = false;
		private List<String> filenamesInTx;
		private boolean waitingForCommitResult = false;
		private boolean waitingToCommit = false;
		private Queue<Command> txQueue = new LinkedList<Command>();
		private FileCommandExecutor cmdQueue;
		private TDFSNode node;

		// TODO: HIGH: Fix how these things reference eachother

		public TransactionQueue(TDFSNode node, FileCommandExecutor cmdQueue) {
			this.node = node;
			this.cmdQueue = cmdQueue;
		}

		public void execute(Command command) {
			if (waitingForCommitResult) {
				txQueue.add(command);
			} else if (command instanceof StartCommand) {
				if (inTx) {
					// TODO: HIGH: fix logging...
					Logger.error(node, "already in transaction");
				} else {
					inTx = true;
					StartCommand startCommand = (StartCommand) command;
					this.filenamesInTx = startCommand.filenames;
					command.execute(node, node.logFS);
				}
			} else if (command instanceof AbortCommand
					|| command instanceof CommitCommand) {
				if (!inTx) {
					Logger.error(node, "not in transaction");
				} else {
					if (cmdQueue.empty()) {
						inTx = false;
						waitingForCommitResult = true;
						this.filenamesInTx = null;
						command.execute(node, node.logFS);
					} else {
						waitingToCommit = true;
						txQueue.add(command);
					}
				}
			} else { // FileCommand
				FileCommand fc = (FileCommand) command;
				if (inTx && !filenamesInTx.contains(fc.filename)) {
					node.printError(fc.filename + " not declared in txstart");
				} else {
					cmdQueue.execute(fc);
				}
			}
		}

		public void nextTx() {
			waitingForCommitResult = false;
			Queue<Command> oldTxQueue = this.txQueue;
			this.txQueue = new LinkedList<Command>();
			Command c;
			while ((c = oldTxQueue.poll()) != null) {
				execute(c);
			}
		}

		public void next(String filename) {
			cmdQueue.next(filename);
			if (waitingToCommit && cmdQueue.empty()) {
				txQueue.poll().execute(node, node.logFS);
			}
		}
	}

	/**
	 * A graph of dependent commands
	 */
	private static class CommandGraph {
		private Command command;
		private int locks = 0;
		private List<CommandGraph> children;
		private FileCommandExecutor commandQueue;

		public void execute() {
			if (command instanceof FileCommand) {
				commandQueue.execute((FileCommand) command);
			} else {
				// TODO: HIGH: Do something transactional
			}
		}

		public void next() {
			for (CommandGraph node : children) {
				node.parentFinished();
			}
		}

		public void parentFinished() {
			locks--;
			if (locks == 0) {
				execute();
			}
		}
	}

	TransactionQueue txQueue;

	private int coordinatorCount;

	private Map<String, List<Integer>> fileListeners;

	/**
	 * PAXOS Structures
	 */

	/**
	 * Learner only: Number of acceptors that have contacted the learner about
	 * accepting a particular N,V pair
	 */
	private Map<String, Integer> acceptorsResponded;

	/**
	 * Proposer only: Number of acceptors that have responded with a promise
	 */
	private Map<String, Integer> promisesReceived;

	/**
	 * Last proposal number promised for a given file
	 */
	private Map<String, Integer> lastProposalNumberPromised;

	private Map<String, Integer> lastProposalNumbersSent;

	private LogFS logFS;

	@Override
	public void start() {
		this.logFS = new LogFileSystem();
		this.lastProposalNumbersSent = new HashMap<String, Integer>();
		this.txQueue = new TransactionQueue(this, new FileCommandExecutor());

		// TODO: HIGH: Coordinator count config
		this.coordinatorCount = 4;

		// Paxos
		this.acceptorsResponded = new HashMap<String, Integer>();
		this.lastProposalNumberPromised = new HashMap<String, Integer>();
	}

	private static String commandDelim = " ";

	@Override
	public void onCommand(String line) {
		int delim = line.indexOf(commandDelim);
		String command = line.substring(0, delim);
		command = command.substring(0, 1).toUpperCase()
				+ command.substring(1).toLowerCase();
		Class<?> cmdClass;
		try {
			cmdClass = Class.forName(command);
		} catch (ClassNotFoundException e) {
			printError(e);
			return;
		}

		line = line.substring(delim + commandDelim.length());

		try {
			Command c;
			if (StartCommand.class.isAssignableFrom(cmdClass)) {
				String[] filenames = line.split(commandDelim);

				Class<?>[] consArgs = { List.class };
				Constructor<?> constructor = cmdClass.getConstructor(consArgs);
				Object[] args = { filenames };
				c = (Command) constructor.newInstance(args);
			} else if (WriteCommand.class.isAssignableFrom(cmdClass)) {
				delim = line.indexOf(commandDelim);
				String filename = line.substring(0, delim);
				String contents = line.substring(delim + commandDelim.length());

				Class<?>[] consArgs = { String.class };
				Constructor<?> constructor = cmdClass.getConstructor(consArgs);
				Object[] args = { filename, contents };
				c = (Command) constructor.newInstance(args);
			} else if (FileCommand.class.isAssignableFrom(cmdClass)) {
				String filename = line;

				Class<?>[] consArgs = { String.class, String.class };
				Constructor<?> constructor = cmdClass.getConstructor(consArgs);
				Object[] args = { filename };
				c = (Command) constructor.newInstance(args);
			} else {
				throw new RuntimeException("failed to create command");
			}
			txQueue.execute(c);
		} catch (Exception e) {
			printError(e);
		}

	}

	/**
	 * Checks if the node is listening on this filename already. If it is, then
	 * it proceeds with preparing and sending a message. If not, then it returns
	 * and requests to start listening
	 * 
	 * @param filename
	 *            The filename
	 * @param op
	 *            The operation
	 */
	public void checkIfListening(String filename, LogEntry op) {
		if (listeningAlready(filename)) {
			int nextOperation = -1;
			try {
				nextOperation = logFS.nextLogNumber(filename);
			} catch (NotListeningException e) {
				Logger.error(this, e);
			}
			Proposal proposal = null;
			try {
				proposal = new Proposal(op, filename, nextOperation,
						nextProposalNumber(filename));
			} catch (NotListeningException e) {
				Logger.error(this, e);
			}
			prepare(addr, proposal.pack());
		}
	}

	/**
	 * Checks whether a node has joined the paxos group for a file already. If
	 * it hasn't, it automatically sends the join request.
	 * 
	 * @param filename
	 *            The filename for the paxos group
	 * @return True if the node has joined already, false otherwise
	 */
	public boolean listeningAlready(String filename) {
		if (!logFS.isListening(filename)) {
			// TODO: High: Queue request or something
			Join(filename);
			return false;
		}
		return true;

	}

	/**
	 * Attempts to listen in on a paxos group for the given file. If this node
	 * is supposed to be the coordinator for that file, attempts to create the
	 * group.
	 * 
	 * @param filename
	 *            The file
	 */
	public void Join(String filename) {

		int coordinator = hashFilename(filename);
		if (getParticipants(filename).contains(addr)) {
			try {
				logFS.createGroup(filename);
				for (Integer next : getParticipants(filename)) {
					if (next != addr)
						RIOSend(next, MessageType.CreateGroup,
								Utility.stringToByteArray(filename));
				}
			} catch (AlreadyParticipatingException e) {
				Logger.error(this, e);
			}
		}
		RIOSend(coordinator, MessageType.RequestToListen,
				Utility.stringToByteArray(filename));
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
			Method handler;
			Object[] args = { from, null };
			try {
				handler = handlingClass.getMethod("receive" + type.name(),
						paramTypes);
				args[1] = msgString;
			} catch (Exception e) {
				paramTypes[1] = byte[].class;
				handler = handlingClass.getMethod("receive" + type.name(),
						paramTypes);
				args[1] = msg;
			}
			handler.invoke(instance, args);
		} catch (Exception e) {
			printError(e);
		}
	}

	/**
	 * Relies on participants being static for any given operation number
	 */
	private int nextProposalNumber(String filename)
			throws NotListeningException {
		List<Integer> participants = getParticipants(filename);
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

	public int hashFilename(String filename) {
		return filename.hashCode() % coordinatorCount;
	}

	public List<Integer> getParticipants(String filename) {
		ArrayList<Integer> list = new ArrayList<Integer>();
		int baseAddr = hashFilename(filename);
		for (int i = 0; i < coordinatorCount; i++) {
			list.add(baseAddr + i);
		}
		return list;
	}

	/**
	 * Sends a prepare request to all acceptors in this Paxos group.
	 * 
	 * @param proposal
	 *            The proposal to send
	 */
	public void prepare(int from, byte[] msg) {
		Proposal p = new Proposal(msg);

		if (p.proposalNumber == -1) {
			try {
				p.proposalNumber = nextProposalNumber(p.filename);
			} catch (NotListeningException e) {
				Logger.error(this, e);
			}
		}

		List<Integer> participants = null;
		try {
			participants = getParticipants(p.filename);
		} catch (NotListeningException e) { // assuming this is a coordinator, a
											// coordinator should never not be
											// participating
			Logger.error(this, e);
		}

		for (Integer next : participants) {
			if (next != addr)
				RIOSend(next, MessageType.Prepare, p.pack());
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
	public void receiveAccept(int from, String msg) {
		RIOSend(from, MessageType.Accepted, Utility.stringToByteArray(msg));
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
	public void receiveAccepted(int from, String msg) {
		Proposal proposal = new Proposal(Utility.stringToByteArray(msg));
		String filename = proposal.filename;

		List<Integer> participants = null;
		participants = getParticipants(filename);

		Integer responded = acceptorsResponded.get(filename);
		responded = (responded == null) ? 0 : responded + 1;

		if (responded < participants.size() / 2)
			return;

		for (Integer next : participants) {
			if (next != addr)
				RIOSend(next, MessageType.Learned,
						Utility.stringToByteArray(msg));
		}

		responded = 0;

		// TODO: High - write to local log

	}

	/**
	 * The proposer checks if this node is part of the paxos group, and if it's
	 * not it checks whether this proposal is a join. If it is not a join and
	 * the node is not part of the paxos group, then the proposal is rejected.
	 * 
	 * @param prop
	 *            The proposal encapsulated
	 */
	public void receiveRequest(int from, String msg) {

		Proposal p = new Proposal(Utility.stringToByteArray(msg));

		try {
			p.operationNumber = logFS.nextLogNumber(p.filename);
			p.proposalNumber = nextProposalNumber(p.filename);
		} catch (NotListeningException e) {
			// Pass the request along
			try {
				logFS.createGroup(p.filename);
			} catch (AlreadyParticipatingException e1) { // Throw a runtime
															// error - something
															// went seriously
															// wrong
				Logger.error(this, e1);
				throw new RuntimeException();
			}
			int address = hashFilename(p.filename);
			for (int i = 0; i < coordinatorCount; i++) {
				address = address + i;
				if (address != addr)
					RIOSend(address, MessageType.CreateGroup,
							Utility.stringToByteArray(p.filename));
			}
			// fall through and start the proposal
		}
		prepare(from, p.pack());
	}

	/**
	 * A message from another coordinator to this coordinator to create a group
	 * 
	 * @param from
	 *            The sender
	 * @param msg
	 *            The filename
	 */
	public void receiveCreateGroup(int from, String msg) {
		try {
			logFS.createGroup(msg);
		} catch (AlreadyParticipatingException e) {
			Logger.error(this, e);
			// TODO: High:
			// This could happen if a coordinator goes down, and receives a join
			// request from someone when it comes back up.
			// It will assume the group doesn't exist and try to instantiate it,
			// but instead the other coordinators should
			// Try to bring it up to speed
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
	public void receivePrepare(int from, String msg) {

		Proposal proposal = new Proposal(Utility.stringToByteArray(msg));
		int proposalNumber = proposal.proposalNumber;
		int operationNumber = proposal.operationNumber;
		String filename = proposal.filename;
		int largestProposalNumberAccepted = lastProposalNumberPromised
				.get(filename);

		if (proposalNumber <= largestProposalNumberAccepted) {
			String lastProposalNumber = lastProposalNumberPromised
					.get(filename) + "";
			RIOSend(from, MessageType.PromiseDenial,
					Utility.stringToByteArray(lastProposalNumber));
			return;
		}

		try {
			if (logFS.nextLogNumber(filename) >= operationNumber) {
				RIOSend(from, MessageType.OldOperation,
						logFS.getLogEntry(filename, operationNumber).pack());

				// TODO High: Don't force them to resend every operation number,
				// somehow update them to latest version
				// Check if forgotten operation
				return;
			}
		} catch (NotListeningException e) {
			Logger.error(this, e);
		} catch (NoSuchOperationNumberException e) {
			Logger.error(this, e);
		}

		largestProposalNumberAccepted = proposalNumber;
		RIOSend(from, MessageType.Promise,
				Utility.stringToByteArray(proposalNumber + ""));

	}

	public void sendFile(String filename) {

	}

	/**
	 * The proposer receives responses from acceptors, and decides whether to
	 * proceed to the accept sending based upon whether it receives responses
	 * from a quorum. It will do nothing until it receives a quorum - this is
	 * acceptable behavior.
	 * 
	 */
	public void receivePromise(int from, String msg) {

		Proposal proposal = new Proposal(Utility.stringToByteArray(msg));
		String filename = proposal.filename;
		List<Integer> participants = null;

		participants = getParticipants(filename);

		Integer responded = promisesReceived.get(filename);
		responded = (responded == null) ? 0 : responded + 1;

		if (responded < participants.size() / 2)
			return;

		for (Integer next : participants) {
			if (next != addr)
				RIOSend(next, MessageType.Learned,
						Utility.stringToByteArray(msg));
		}

		if (responded < (participants.size() / 2))
			return;

		for (Integer i : participants) {
			if (i != addr) {
				RIOSend(i, MessageType.Accept, Utility.stringToByteArray(msg));
			}
		}

		responded = 0;

	}

	/**
	 * A request from a node to listen on a given filename
	 * 
	 * @param from
	 *            Who sent the request
	 * @param msg
	 *            The proposal, in packed form
	 */
	public void receciveRequestToListen(int from, String filename) {
		List<Integer> list = fileListeners.get(filename);
		if (list == null)
			list = new ArrayList<Integer>();
		list.add(from);
	}

	/**
	 * This proposal is now learned, so put it in the log
	 * 
	 * @param msg
	 *            The proposal, as a byte array
	 */
	public void receiveLearned(int from, byte[] msg) {
		Proposal p = new Proposal(msg);
		logFS.writeLogEntry(p.filename, p.operationNumber, p.operation);

		// inform listeners
		List<Integer> listeners = fileListeners.get(p.filename);
		for (Integer i : listeners) {
			RIOSend(i, MessageType.Learned, msg);
		}

		if ((p.operation instanceof TXCommitLogEntry || p.operation instanceof TXAbortLogEntry)
				&& txQueue.inTx && txQueue.filenamesInTx.contains(p.filename)) {
			txQueue.nextTx();

		}
		
		if (txQueue.cmdQueue.executingOn(p.filename)){
			txQueue.next(p.filename);
		}
		/*
		 * TODO: HIGH: "Unlock" completed commands via
		 * Command/TXQueue/CommandGraph
		 */

	}
}
