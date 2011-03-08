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
	 * TODO: HIGH: Listen commands should probably just send requestToListens to
	 * themselves even if the coordinator is the one doing the request for
	 * simplicity
	 * 
	 * TODO: HIGH: Change -1 int special cases in Commands to null Integer cases
	 * 
	 * TODO: HIGH: Verify not proposing things on txing files
	 * 
	 * TODO: HIGH: Node count config commands
	 * 
	 * TODO: HIGH: Is there any reason coordinators should auto-listen to all
	 * files they coordinate? I don't think it's necessary.
	 * 
	 * TODO: HIGH: If a node prepares a txstart, but that txstart is not
	 * accepted (possibly because another value has been accepted for that
	 * proposal number), then the node will think it is in a tx even though it
	 * is not, because the txstart was not accepted.
	 * 
	 * TODO: HIGH: When listener joins group, it needs the latest copy of the
	 * log
	 * 
	 * TODO: HIGH: Coordinator rebuild active file list on restart
	 * 
	 * TODO: HIGH: Cleanup Paxos (filename, opNum) -> X data structures when we
	 * learn entries for those keys.
	 * 
	 * TODO: HIGH: Verify 2PC Coordinator correctness
	 * 
	 * TODO: HIGH: Update Paxos comments
	 * 
	 * TODO: HIGH: author headers at the top of each file
	 * 
	 * TODO: HIGH: Verify Paxos Flow (Synoptic?):
	 * 
	 * (Req -1>) Prepare -> OldOp -1> (done) | PromiseDenial -1> (done) |
	 * Promise -1> Accept -> AcceptDenial -1> (done) | Accepted -1> Learned ->
	 * (done)
	 * 
	 * TODO: re-request to listen when notifying coordinator goes down: maybe
	 * detect if we're getting Paxos messages from other coordinators except the
	 * coordinator we expect to be notifying us of log changes
	 * 
	 * TODO: abortCommands for CommandNodes should propose TXTryAborts, not
	 * TXAborts
	 * 
	 * TODO: support concurrent transactions
	 * 
	 * TODO: Test w/ handshakes
	 * 
	 * TODO: Logger config
	 * 
	 * TODO: txaborts and txcommits currently use commandGraph.addCheckpoint,
	 * which means they don't happen parallel for no good reason. They should
	 * depend on all tails, and everything added after them should depend on
	 * them, but they shouldn't depend on each other.
	 * 
	 * TODO: Support a StopListening command clients can use when they lose
	 * interest in a file. This way they can also request to listen from a
	 * second coordinator if one becomes unresponsive and clean up when the
	 * first becomes responsive again
	 * 
	 * TODO: Support multiple 2PC coordinators for reliability - cold
	 * replacements with leases should be fine
	 * 
	 * TODO: Contention friendly ops - coordinator declare lead proposer
	 * 
	 * TODO: OPT: GC logs
	 * 
	 * TODO: Lead proposer support - send MessageType.Request to elected lead
	 * proposer for lively Paxos
	 * 
	 * TODO: Consistent Hashing?
	 * 
	 * TODO: Support dynamic coordinator groups -
	 * 
	 * Interesting idea: include the coordinator count as part of the filename
	 * (not necessarily literally), and then use a biased hash function so that
	 * the expected distribution of hash codes stays ~uniform even when the
	 * number of coordinators changes: as long as nodes usually know the actual
	 * total number of coordinators, the distribution of hash codes should be
	 * good.
	 */

	/**
	 * Delimiter used by the parsers (client to library)
	 */
	private static final String commandDelim = " ";

	/**
	 * The total, static number of coordinators with addresses
	 * [0,coordinatorCount). Most of these should be alive at any given time.
	 */
	private static final int coordinatorCount = 4;

	/**
	 * The static number of coordinators per file.
	 * 
	 * Progress can be made as long as floor(coordinatorsPerFile / 2) + 1
	 * coordinators are alive
	 */
	private static final int coordinatorsPerFile = 3;

	/**
	 * The maximum number of nodes allowed in the system.
	 * 
	 * Node with addresses [twoPCCoordinatorAddress + 1, maxTotalNodeCount) are
	 * proposers/clients but not coordinators.
	 */
	private static final int maxTotalNodeCount = 10;

	/**
	 * We currently only support a single 2PC coordinator with this address
	 */
	static final int twoPCCoordinatorAddress = coordinatorCount + 1;

	/**
	 * Time between when the 2PC Coordinator learns about a transaction starting
	 * and will abort it
	 */
	private static final int txTimeout = 50;

	/**
	 * Graph of commands used by clients for concurrency control
	 */
	CommandGraph commandGraph;

	/**
	 * In memory logs of recent file changes
	 */
	protected LogFS logFS;

	/**
	 * Encapsulates persistent paxos data structures
	 */
	private PersistentPaxosState paxosState;

	/**
	 * Array of transacting filenames used by the library to aggressively fail
	 * 
	 * Used by the client right now, but wouldn't be visible in a real
	 * implementation.
	 */
	private String[] transactingFiles;

	/**
	 * Learner only: Number of acceptors that have contacted the learner about
	 * accepting a particular N,V pair
	 */
	private Map<Tuple<String, Integer>, Integer> acceptorsResponded;

	/**
	 * List of addresses this coordinator passes on changes to when it learns a
	 * file changes
	 */
	Map<String, List<Integer>> fileListeners;

	/**
	 * Last proposal number prepared for a given file
	 */
	private Map<Tuple<String, Integer>, Integer> lastProposalNumbersSent;

	/**
	 * Proposer only: Number of acceptors that have responded with a promise
	 */
	private Map<Tuple<String, Integer>, Integer> promisesReceived;

	/**
	 * Proposer only: Highest proposal value accepted from received promises
	 */
	private Map<Tuple<String, Integer>, Proposal> highestProposalReceived;

	/**
	 * 2PC
	 * 
	 * Ties individual files to transactions, so for example, if a client is
	 * doing a transaction on files f1, f2, f3, the map would look like:
	 * 
	 * f1 -> f1, f2, f3
	 * 
	 * f2 -> f1, f2, f3
	 * 
	 * etc.
	 */
	private Map<String, String[]> fileTransactionMap;

	/**
	 * Simple hash function from filenames to addresses in [0,coordinatorCount)
	 */
	private static int hashFilename(String filename) {
		return filename.hashCode() % coordinatorCount;
	}

	@Override
	public void start() {
		// Client
		this.commandGraph = new CommandGraph(this);
		this.getCoordinatorOffset = new HashMap<String, Integer>();
		this.logFS = new LogFileSystem();
		this.transactingFiles = null;

		// Paxos
		this.acceptorsResponded = new HashMap<Tuple<String, Integer>, Integer>();
		this.fileListeners = new HashMap<String, List<Integer>>();
		this.lastProposalNumbersSent = new HashMap<Tuple<String, Integer>, Integer>();
		this.paxosState = new PersistentPaxosState(this);
		this.promisesReceived = new HashMap<Tuple<String, Integer>, Integer>();
		this.highestProposalReceived = new HashMap<Tuple<String, Integer>, Proposal>();

		// 2PC
		fileTransactionMap = new HashMap<String, String[]>();

	}

	/**
	 * Extracts the first word in line as the cmd and calls cmdParser via
	 * dynamic dispatch
	 */
	@Override
	public void onCommand(String line) {
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

	private String username;

	public void addFriendParser(Tokenizer t) {
		String friendName = t.next();

		String[] filenames = { username, friendName };
		txstart(filenames);
		try {
			append(username + ".friends", friendName);
			append(friendName + ".friends", username);
			txcommit();
		} catch (Exception e) {
			txabort();
		}
	}

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

	public void txabortParser(Tokenizer t) {
		if (transactingFiles != null) {
			txabort();
		} else {
			printError("not in transaction");
		}
	}

	public void txcommitParser(Tokenizer t) {
		if (transactingFiles != null) {
			txcommit();
		} else {
			printError("not in transaction");
		}
	}

	public void txstartParser(Tokenizer t) {
		if (transactingFiles == null) {
			transactingFiles = t.rest().split(commandDelim);
			txstart(transactingFiles);
		} else {
			printError("already in transaction");
		}
	}

	public void append(String filename, String contents) {
		checkListen(filename, new AppendCommand(filename, contents, this.addr));
	}

	public void create(String filename) {
		checkListen(filename, new CreateCommand(filename, this.addr));
	}

	public void delete(String filename) {
		checkListen(filename, new DeleteCommand(filename, this.addr));
	}

	public void get(String filename) {
		checkListen(filename, new GetCommand(filename, this.addr));
	}

	public void put(String filename, String contents) {
		checkListen(filename, new PutCommand(filename, contents, this.addr));
	}

	private void checkListen(String filename, FileCommand after) {
		List<Command> abortCommands = null;
		if (transactingFiles != null) {
			abortCommands = new ArrayList<Command>(transactingFiles.length);
			for (String tf : transactingFiles) {
				abortCommands.add(new AbortCommand(transactingFiles, tf,
						this.addr));
			}
		}

		if (!logFS.isListening(filename)) {
			CommandNode l = commandGraph.addCommand(new ListenCommand(filename,
					this.addr), false, abortCommands);
			commandGraph.addCommand(after, false, abortCommands);
			l.execute();
			if (getCoordinators(filename).contains(this.addr)) {
				// listen done
				commandGraph.done(new CommandKey(filename, -1, -1));
			}
		} else {
			commandGraph.addCommand(after).execute();
		}
	}

	public void txabort() {
		if (transactingFiles != null) {
			for (String filename : transactingFiles) {
				checkListen(filename, new AbortCommand(transactingFiles,
						filename, this.addr));
			}
			transactingFiles = null;
		} else {
			printError("not in a transaction");
		}
	}

	public void txcommit() {
		if (transactingFiles != null) {
			for (String filename : transactingFiles) {
				checkListen(filename, new CommitCommand(transactingFiles,
						filename, this.addr));
			}
			transactingFiles = null;
		} else {
			printError("not in transaction");
		}
	}

	public void txstart(String[] filenames) {
		if (transactingFiles == null) {
			transactingFiles = filenames;
			// sort filenames
			List<String> sorted = new ArrayList<String>(transactingFiles.length);
			for (String filename : transactingFiles) {
				sorted.add(filename);
			}
			Collections.sort(sorted);
			transactingFiles = sorted.toArray(transactingFiles);

			for (String filename : transactingFiles) {
				/*
				 * uses addCheckpoint, so each listen/start depends on the
				 * previous txstart and they get executed and finish in order
				 */
				checkListen(filename, new StartCommand(transactingFiles,
						filename, this.addr));
			}
		} else {
			printError("already in transaction");
		}
	}

	/**
	 * non-FileCommand version used for transactions
	 */
	private void checkListen(String filename, Command after) {
		if (!logFS.isListening(filename)) {
			CommandNode l = commandGraph
					.addCommand(new ListenCommand(filename));
			commandGraph.addCheckpoint(after);
			l.execute();
			if (getCoordinators(filename).contains(this.addr)) {
				commandGraph.filenameDone(filename, -1, -1);
			}
		} else {
			commandGraph.addCheckpoint(after).execute();
		}
	}

	/**
	 * Dynamically dispatches messages to receiveType(int, String|byte[])
	 * 
	 * Routes all messages to learn for 2PC coordinators
	 */
	@Override
	public void onRIOReceive(Integer from, MessageType type, byte[] msg) {
		// 2PC handler
		if (addr == twoPCCoordinatorAddress) {
			learn(from, msg);
			return;
		}

		// route message
		try {
			Class<?> handlingClass = this.getClass();
			Class<?>[] paramTypes = { int.class, String.class };
			Method handler;
			Object[] args = { from, null };
			try {
				// look for receive<cmd>(int, String)
				handler = handlingClass.getMethod("receive" + type.name(),
						paramTypes);
				args[1] = Utility.byteArrayToString(msg);
			} catch (Exception e) {
				// look for receive<cmd>(int, bytep[])
				paramTypes[1] = byte[].class;
				handler = handlingClass.getMethod("receive" + type.name(),
						paramTypes);
				args[1] = msg;
			}
			handler.invoke(this, args);
		} catch (InvocationTargetException e) {
			printError(e.getCause());
		} catch (Exception e) {
			printError(e);
		}
	}

	/**
	 * Since there is no globally consistent list of proposers, we assign
	 * proposal numbers round robin to each node in the system
	 */
	public int nextProposalNumber(String filename, int operationNumber)
			throws NotListeningException {
		Integer lastNumSent = lastProposalNumbersSent
				.get(new Tuple<String, Integer>(filename, operationNumber));
		int number;
		if (lastNumSent == null) {
			// use address as offset
			number = this.addr;
		} else {
			// increment last sent by proposer count
			number = lastNumSent + maxTotalNodeCount;
		}
		lastProposalNumbersSent.put(new Tuple<String, Integer>(filename,
				operationNumber), number);
		return number;
	}

	public List<Integer> getCoordinators(String filename) {
		ArrayList<Integer> list = new ArrayList<Integer>();
		int baseAddr = hashFilename(filename);
		for (int i = 0; i < coordinatorsPerFile; i++) {
			list.add((baseAddr + i) % coordinatorCount);
		}
		return list;
	}

	private Map<String, Integer> getCoordinatorOffset;

	/**
	 * Returns a coordinator for filename. Multiple calls to this method will
	 * cycle through all possible coordinators in order.
	 */
	public int getCoordinator(String filename) {
		Integer offset = getCoordinatorOffset.get(filename);
		if (offset == null) {
			offset = 0;
		} else {
			offset = (offset + 1) % coordinatorsPerFile;
		}
		getCoordinatorOffset.put(filename, offset);
		return (hashFilename(filename) + offset) % coordinatorCount;
	}

	/**
	 * Sends a prepare request to all acceptors in this Paxos group.
	 * 
	 * @param proposal
	 *            The proposal to sendThat is
	 */
	public void prepare(Proposal p) {
		List<Integer> participants = getCoordinators(p.filename);

		byte[] packed = p.pack();

		for (Integer next : participants) {
			RIOSend(next, MessageType.Prepare, packed);
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
		Proposal p = new Proposal(msg);
		if (p.proposalNumber >= paxosState.highestPromised(p.filename,
				p.operationNumber)) {
			paxosState.accept(p);
			List<Integer> coordinators = getCoordinators(p.filename);
			for (int address : coordinators) {
				RIOSend(address, MessageType.Accepted, msg);
			}
		} else {
			// send some denial, maybe an updated promise or just an abort?
		}
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
		Proposal p = new Proposal(msg);

		if (logFS.hasLogNumber(p.filename, p.operationNumber)) {
			return;
		}

		List<Integer> coordinators = getCoordinators(p.filename);

		Integer responded = acceptorsResponded.get(new Tuple<String, Integer>(
				p.filename, p.operationNumber));
		responded = (responded == null) ? 1 : responded + 1;
		acceptorsResponded.put(new Tuple<String, Integer>(p.filename,
				p.operationNumber), responded);

		if (responded > coordinators.size() / 2) {
			// inform listeners iff coordinator for this file
			List<Integer> listeners = fileListeners.get(p.filename);
			if (listeners != null) {
				for (Integer i : listeners) {
					RIOSend(i, MessageType.Learned, msg);
				}
			}
			acceptorsResponded.remove(p.filename);
		}
	}

	/**
	 * The proposer checks if this node is part of the paxos group, and if it's
	 * not it checks whether this proposal is a join. If it is not a join and
	 * the node is not part of the paxos group, then the proposal is rejected.
	 * 
	 */
	public void receiveRequest(int from, byte[] msg) {
		Proposal p = new Proposal(msg);
		try {
			p.operationNumber = logFS.nextLogNumber(p.filename);
			p.proposalNumber = nextProposalNumber(p.filename, p.operationNumber);
		} catch (NotListeningException e) {
			Logger.error(this, e);
		}
		prepare(p);
	}

	/**
	 * A message from another coordinator to this coordinator to create a group
	 * 
	 * @param from
	 *            The sender
	 * @param filename
	 *            The filename
	 */
	public void receiveCreateGroup(int from, String filename) {
		if (!logFS.isListening(filename)) {
			logFS.createGroup(filename);
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
		Proposal p = new Proposal(msg);

		Integer largestProposalNumberPromised = paxosState.highestPromised(
				p.filename, p.operationNumber);
		if (largestProposalNumberPromised == null) {
			largestProposalNumberPromised = -1;
		}

		if (logFS.nextLogNumber(p.filename) > p.operationNumber) {
			LogEntry entry = logFS.getLogEntry(p.filename, p.operationNumber);
			if (entry != null) {
				Proposal r = new Proposal(entry, p.filename, p.operationNumber,
						-1);
				RIOSend(from, MessageType.Learned, r.pack());
				// TODO: HIGH: Get them all the way up to date
			} else {
				// TODO: HIGH: GC'd or missing operation, do something
			}
			return;
		} else if (p.proposalNumber <= largestProposalNumberPromised) {
			int highestProposalNumber = paxosState.highestPromised(p.filename,
					p.operationNumber);

			while (p.proposalNumber < highestProposalNumber) {
				// HACK HACK HACk
				p.proposalNumber += maxTotalNodeCount;
			}

			RIOSend(from, MessageType.PromiseDenial, p.pack());
			return;
		} else {
			paxosState.promise(p.filename, p.operationNumber, p.proposalNumber);
			Proposal highestAccepted = paxosState.highestAccepted(p.filename,
					p.operationNumber);
			if (highestAccepted != null) {
				highestAccepted.originalProposal = p.proposalNumber;
				RIOSend(from, MessageType.Promise, highestAccepted.pack());
			} else {
				RIOSend(from, MessageType.Promise, p.pack());
			}
		}
	}

	/**
	 * The proposer receives responses from acceptors, and decides whether to
	 * proceed to the accept sending based upon whether it receives responses
	 * from a quorum. It will do nothing until it receives a quorum - this is
	 * acceptable behavior.
	 * 
	 */
	public void receivePromise(int from, byte[] msg) {
		Proposal p = new Proposal(msg);
		List<Integer> coordinators = getCoordinators(p.filename);

		Integer responded = promisesReceived.get(new Tuple<String, Integer>(
				p.filename, p.operationNumber));
		responded = (responded == null) ? 1 : responded + 1;
		promisesReceived.put(new Tuple<String, Integer>(p.filename,
				p.operationNumber), responded);

		Proposal highestResponse = highestProposalReceived
				.get(new Tuple<String, Integer>(p.filename, p.operationNumber));
		// BAM!
		if (highestResponse == null
				|| (p.originalProposal != -1 && (highestResponse.originalProposal == -1 || highestResponse.proposalNumber < p.proposalNumber))) {
			highestResponse = p;
			highestProposalReceived.put(new Tuple<String, Integer>(p.filename,
					p.operationNumber), p);
		}

		if (responded > coordinators.size() / 2) {
			byte[] packed = highestResponse.pack();
			for (Integer i : coordinators) {
				RIOSend(i, MessageType.Accept, packed);
			}
			promisesReceived.remove(new Tuple<String, Integer>(p.filename,
					p.operationNumber));
		}
	}

	public void receivePromiseDenial(int from, byte[] msg) {
		// PACK, UNPACK!
		prepare(new Proposal(msg));
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
		if (!logFS.isListening(filename)) { // If the group doesn't exist
			new ListenCommand(filename, this.addr).execute(this);
		}
		fileListeners.get(filename).add(from);
		RIOSend(from, MessageType.AddedListener,
				Utility.stringToByteArray(filename));
	}

	public void receiveAddedListener(int from, String filename) {
		commandGraph.done(new CommandKey(filename, -1, -1));
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

		// tell the commandGraph to finish commands it might be executing
		if (p.operation instanceof TXAbortLogEntry
				|| p.operation instanceof TXCommitLogEntry
				|| p.operation instanceof TXStartLogEntry) {
			if (commandGraph.done(new CommandKey(p.filename, this.addr))) {
				printVerbose("finished: " + p.operation.toString());
			}
		} else {
			if (commandGraph.done(new CommandKey(p.filename, p.operationNumber,
					p.proposalNumber))) {
				printVerbose("finished: " + p.operation.toString());
			}
		}
	}

	/**
	 * 2PC Coordinator Methods
	 */

	/**
	 * The 2PC coordinator learns about a operation. It logs it to its own
	 * logFS, but also tries to discern whether the given command is something
	 * it should pay attention to - txstarts, commits, and aborts.
	 * 
	 * @param from
	 *            The coordinator who sent this message
	 * @param msg
	 *            The proposal, packed
	 */
	public void learn(int from, byte[] msg) {
		Proposal p = new Proposal(msg);
		if (!logFS.isListening(p.filename)) {
			logFS.createGroup(p.filename);
		}
		logFS.writeLogEntry(p.filename, p.operationNumber, p.operation);
		String[] txFiles = fileTransactionMap.get(p.filename);

		// check for transactions

		if (p.operation instanceof TXStartLogEntry) {
			TXStartLogEntry txCommand = (TXStartLogEntry) p.operation;

			// check for duplicate learns
			if (txFiles != null && txFiles.equals(txCommand.filenames))
				return;

			// start callback
			addTxTimeout(p.filename);

			// add files
			String[] files = txCommand.filenames;
			int client = txCommand.address;
			if (txFiles != null) { // client didn't abort or commit last tx
				Logger.error(this, "Client: " + client
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
			for (String file : txFiles) {
				fileTransactionMap.put(file, null);
			}
		}

		else if (p.operation instanceof TXTryAbortLogEntry) {
			abortClientTx(p.filename);
		}
	}

	/**
	 * Aborts all tx for a client, assuming that each file is used in a tx at
	 * most once. That is, this method will cause all sorts of problems if
	 * multiple clients are allowed to start a tx on the same file.
	 * 
	 * @param filename
	 *            The filename key to abort
	 */
	public void abortClientTx(String filename) {
		String[] files = fileTransactionMap.get(filename);
		if (files == null)
			return; // Assume the client must have already aborted or committed
		// this tx
		for (String file : files) {
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
		addTimeout(cb, TDFSNode.txTimeout);
	}

	/**
	 * Creates a proposal using the appropriate log entry type, and send to each
	 * coordinator for the list files
	 * 
	 * @param operation
	 *            The operation to perform
	 * @param files
	 *            The list of files involved in this tx
	 */
	public void createProposal(LogEntry operation, String[] files) {
		for (String file : files) {
			List<Integer> coordinators = getCoordinators(file);
			Proposal newProposal = new Proposal(new TXCommitLogEntry(files),
					file, logFS.nextLogNumber(file), nextProposalNumber(file,
							logFS.nextLogNumber(file)));

			byte[] packed = newProposal.pack();
			for (Integer addr : coordinators) {
				RIOSend(addr, MessageType.Prepare, packed);
			}
		}
	}
}
