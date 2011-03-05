package edu.washington.cs.cse490h.tdfs;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.Utility;
import edu.washington.cs.cse490h.tdfs.CommandGraph.CommandNode;

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
	 * BEGIN 2PC structures
	 */
	private static final int TwoPCCoordinatorAdress = 100;
	
	private final int TXTIMEOUT = 20;
	
	private Map<String, String[]> fileTransactionMap;

	/**
	 * END 2PC structures
	 */
	
	CommandGraph commandGraph;

	private static int coordinatorCount = 4;

	private static int coordinatorsPerFile = 3;

	// TODO: HIGH: Encapsulate Paxos state in objects for handoff Commands

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

	LogFS logFS;

	@Override
	public void start() {
		this.logFS = new LogFileSystem();
		this.lastProposalNumbersSent = new HashMap<String, Integer>();
		this.commandGraph = new CommandGraph(this);

		// Paxos
		this.acceptorsResponded = new HashMap<String, Integer>();
		this.fileListeners = new HashMap<String, List<Integer>>();
		this.lastProposalNumbersSent = new HashMap<String,Integer>();
		this.lastProposalNumberPromised = new HashMap<String, Integer>();
		this.promisesReceived = new HashMap<String, Integer>();
		
		// 2PC
		fileTransactionMap = new HashMap<String, String[]>();
		
	}

	private static String commandDelim = " ";

	@Override
	public void onCommand(String line) {
		// TODO: HIGH: Parse and call <cmd>Handler
		Tokenizer t = new Tokenizer(line, commandDelim);
		String command = t.next();
		if (command == null) {
			printError("no command found");
			return;
		}
		command = command.toLowerCase();

		try {
			Class<?>[] paramTypes = { Tokenizer.class };
			Method handler = this.getClass().getMethod(command + "Parser",
					paramTypes);
			Object[] args = { t };
			handler.invoke(this, args);
		} catch (InvocationTargetException e) {
			printError(e.getCause());
		} catch (Exception e) {
			printError("invalid command: " + line);
		}
	}

	/*
	 * TODO: HIGH: Re-enstate handlers, but with better args so they are caller
	 * from other, higher level handlers. This means we have to do parsing
	 * somewhere else.
	 */

	public void appendParser(Tokenizer t) {
		String filename = t.next();
		String contents = t.rest();
		append(filename, contents);
	}

	public void createParser(Tokenizer t) {
		String filename = t.next();
		create(filename);
	}

	public void deleteParser(Tokenizer t) {
		String filename = t.next();
		delete(filename);
	}

	public void getParser(Tokenizer t) {
		String filename = t.next();
		get(filename);
	}

	public void putParser(Tokenizer t) {
		String filename = t.next();
		String contents = t.rest();
		put(filename, contents);
	}

	private String[] transactingFiles;

	public void txabortParser(Tokenizer t) {
		txabort(transactingFiles);
	}

	public void txcommitParser(Tokenizer t) {
		txcommit(transactingFiles);
	}

	public void txstartParser(Tokenizer t) {
		String[] filenames = t.rest().split(commandDelim);
		txstart(filenames);
	}

	public void append(String filename, String contents) {
		checkListen(filename, new AppendCommand(filename, contents));
	}

	public void create(String filename) {
		checkListen(filename, new CreateCommand(filename));
	}

	public void delete(String filename) {
		checkListen(filename, new DeleteCommand(filename));
	}

	public void get(String filename) {
		checkListen(filename, new GetCommand(filename));
	}

	public void put(String filename, String contents) {
		checkListen(filename, new PutCommand(filename, contents));
	}

	public void txabort(String[] filenames) {
		for (String filename : filenames) {
			checkListen(filename, new AbortCommand(filenames, filename));
		}
		transactingFiles = null;
	}

	public void txcommit(String[] filenames) {
		for (String filename : filenames) {
			checkListen(filename, new CommitCommand(filenames, filename));
		}
		transactingFiles = null;
	}

	public void txstart(String[] filenames) {
		for (String filename : filenames) {
			checkListen(filename, new StartCommand(filenames, filename));
		}
		transactingFiles = filenames;
	}

	private void checkListen(String filename, FileCommand after) {
		if (!logFS.isListening(filename)) {
			CommandNode l = commandGraph
					.addCommand(new ListenCommand(filename));
			commandGraph.addCommand(after);
			l.execute();
		} else {
			commandGraph.addCommand(after).execute();
		}
	}

	/**
	 * Non-FileCommand version
	 */
	private void checkListen(String filename, Command after) {
		if (!logFS.isListening(filename)) {
			CommandNode l = commandGraph
					.addCommand(new ListenCommand(filename));
			commandGraph.addCheckpoint(after);
			l.execute();
		} else {
			commandGraph.addCheckpoint(after).execute();
		}
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
		logFS.createGroup(filename);
		if (getParticipants(filename).contains(addr)) {
			try {
				for (Integer next : getParticipants(filename)) {
					if (next != addr)
						RIOSend(next, MessageType.CreateGroup,
								Utility.stringToByteArray(filename));
				}
			} catch (AlreadyParticipatingException e) {
				Logger.error(this, e);
			}
		} else {
			RIOSend(coordinator, MessageType.RequestToListen,
					Utility.stringToByteArray(filename));
		}
	}

	@Override
	public void onRIOReceive(Integer from, MessageType type, byte[] msg) {
		String msgString = Utility.byteArrayToString(msg);

		// TDFS handles all non-RIO messages right now
		Object instance = this;
		
		// 2PC handler
		if (addr == TwoPCCoordinatorAdress){
			learn(from, msg);
		}

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
		} catch (InvocationTargetException e) {
			printError(e.getCause());
		} catch (Exception e) {
			printError(e);
		}
	}

	/**
	 * Relies on participants being static for any given operation number
	 */
	public int nextProposalNumber(String filename) throws NotListeningException {
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

	public static int hashFilename(String filename) {
		return filename.hashCode() % coordinatorCount;
	}

	public static List<Integer> getParticipants(String filename) {
		ArrayList<Integer> list = new ArrayList<Integer>();
		int baseAddr = hashFilename(filename);
		for (int i = 0; i < coordinatorsPerFile; i++) {
			list.add(baseAddr + i);
		}
		return list;
	}

	/**
	 * Sends a prepare request to all acceptors in this Paxos group.
	 * 
	 * @param proposal
	 *            The proposal to sendThat is
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
	public void receiveRequestToListen(int from, String filename) {
		if (!logFS.isListening(filename)){
			logFS.createGroup(filename);
			for (Integer i : getParticipants(filename)){
				RIOSend(i, MessageType.CreateGroup, Utility.stringToByteArray(filename));
			}
		}
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

		/*
		 * TODO: HIGH: Change to use commandGraph if ((p.operation instanceof
		 * TXCommitLogEntry || p.operation instanceof TXAbortLogEntry) &&
		 * txQueue.inTx && txQueue.filenamesInTx.contains(p.filename)) {
		 * txQueue.nextTx(); }
		 * 
		 * if (txQueue.cmdQueue.executingOn(p.filename)) {
		 * txQueue.next(p.filename); }
		 */
	}
	
	// 2PC Coordinator Methods
	/**
	 * The 2PC coordinator learns about a operation. It logs it to its own logFS, but also
	 * tries to discern whether the given command is something it should pay attention to - txstarts,
	 * commits, and aborts.
	 * @param from The coordinator who sent this message
	 * @param msg The proposal, packed
	 */
	public void learn(int from, byte[] msg) {
		Proposal p = new Proposal(msg);
		logFS.writeLogEntry(p.filename, p.operationNumber, p.operation);
		String[] txFiles = fileTransactionMap.get(p.filename);

		// check for transactions

		if (p.operation instanceof TXStartLogEntry) {
			// start callback
			addTxTimeout(p.filename);
			// add files
			String[] files = ((TXStartLogEntry) p.operation).filenames;
			if (txFiles != null) { // client didn't abort or commit last tx
				Logger.error(this, "Client: " + from
						+ " did not commit or abort last tx!");
			}
			for (String file : files) {
				fileTransactionMap.put(file, files);
			}

		}

		else if (p.operation instanceof TXTryCommitLogEntry) {
			// commit to each filename coordinator
			String[] files = ((TXTryCommitLogEntry) p.operation).filenames;
			createProposal(new TXTryCommitLogEntry(files), files);
			for (String file : txFiles){
				fileTransactionMap.put(file, null);
			}
		}

		else if (p.operation instanceof TXTryAbortLogEntry) {
			abortClientTx(p.filename);
		}
	}
	
	/**
	 * Aborts all tx for a client, assuming that each file is used in a tx at most once.
	 * That is, this method will cause all sorts of problems if multiple clients are allowed to
	 * start a tx on the same file.
	 * @param filename The filename key to abort
	 */
	public void abortClientTx(String filename) {
		String[] files = fileTransactionMap.get(filename);
		if (files == null)
			return; // Assume the client must have already aborted or committed this tx
		for (String file : files){
			createProposal(new TXTryAbortLogEntry(files), files);
			fileTransactionMap.put(file, null);
		}
	}

	/**
	 * Adds a transaction timeout for the given client. If the client hasn't
	 * committed their transaction by the time the lease expires, then
	 * 
	 * @param client
	 */
	public void addTxTimeout(String filename) {
		Method cbMethod = null;
		try {
			String[] params = { "java.lang.String" };
			cbMethod = Callback.getMethod("abortClientTx", this, params);
			cbMethod.setAccessible(true); // HACK
		} catch (Exception e) {
			printError(e);
			e.printStackTrace();
		}
		String[] args = { filename };
		Callback cb = new Callback(cbMethod, this, args);
		addTimeout(cb, this.TXTIMEOUT);
	}
	
	
	/**
	 * Creates a proposal using the appropriate log entry type, and send to each coordinator
	 * for the list files
	 * @param operation The operation to perform
	 * @param files The list of files involved in this tx
	 */
	public void createProposal(LogEntry operation, String[] files){
		for (String file : files) {
			List<Integer> coordinators = TDFSNode.getParticipants(file);
			Proposal newProposal = new Proposal(
					new TXCommitLogEntry(files), file,
					logFS.nextLogNumber(file), nextProposalNumber(file));

			for (Integer addr : coordinators){
				RIOSend(addr, MessageType.Prepare, newProposal.pack());
			}
		}
	}
}
