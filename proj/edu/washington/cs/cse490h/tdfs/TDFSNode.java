package edu.washington.cs.cse490h.tdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.Utility;
import edu.washington.cs.cse490h.tdfs.CommandGraph.CommandNode;

public class TDFSNode extends RIONode {

	/*
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
	 * TODO: Our TXs implement synchronized (filenameList) { }, document
	 * 
	 * TODO: Support multiple 2PC coordinators for reliability - cold
	 * replacements with leases should be fine
	 * 
	 * TODO: Support a StopListening command clients can use when they lose
	 * interest in a file. This way they can also request to listen from a
	 * second coordinator if one becomes unresponsive and clean up when the
	 * first becomes responsive again
	 * 
	 * TODO: Test w/ handshakes
	 * 
	 * TODO: Logger config
	 * 
	 * TODO: Contention friendly ops - coordinator declare lead proposer
	 * 
	 * TODO: support concurrent transactions
	 * 
	 * TODO: Clean filenames
	 * 
	 * TODO: OPT: GC logs
	 * 
	 * TODO: Lead proposer support - send requests with MessageType.Request and
	 * elect lead proposer with a lease for lively Paxos
	 * 
	 * TODO: Consistent Hashing / Dynamic Coordinator Groups -
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
	private static int coordinatorCount = 4;

	/**
	 * The static number of coordinators per file.
	 * 
	 * Progress can be made as long as floor(coordinatorsPerFile / 2) + 1
	 * coordinators are alive
	 */
	private static int coordinatorsPerFile = 3;

	/**
	 * The maximum number of nodes allowed in the system.
	 * 
	 * Node with addresses [twoPCCoordinatorAddress + 1, maxTotalNodeCount) are
	 * proposers/clients but not coordinators.
	 */
	private static int maxTotalNodeCount = 10;

	/**
	 * We currently only support a single 2PC coordinator with this address
	 */
	static int twoPCCoordinatorAddress = coordinatorCount;

	/**
	 * Time between when the 2PC Coordinator learns about a transaction starting
	 * and will abort it
	 */
	private static final int txTimeout = 100;

	/**
	 * Graph of commands used by clients for concurrency control
	 */
	CommandGraph commandGraph;

	/**
	 * In memory logs of recent file changes
	 */
	protected LogFS logFS;

	/**
	 * Client only: Set of filenames we think we might not be receiving updates
	 * on anymore, even though we are listening on that file.
	 * 
	 * TODO: HIGH: add filenames to relisten on command retry
	 */
	protected Set<String> relisten;

	/**
	 * Library only: Array of transacting filenames used by the library to
	 * aggressively fail tx commands. Null if not transacting. Should never be
	 * [].
	 * 
	 * Used by the client right now, but wouldn't be visible in a real
	 * implementation.
	 */
	String[] transactingFiles;

	/**
	 * Used by the node to keep track of which files in a transaction have been
	 * committed/aborted
	 */
	private Set<String> resolvedTransactionFiles;

	private String resolvedTransactionKey;

	/**
	 * Learner only: Number of acceptors that have contacted the learner about
	 * accepting a particular ((filename, opNum), value) pair
	 */
	private Map<Tuple<String, Integer>, Integer> acceptorsResponded;

	/**
	 * Learner only: List of addresses this coordinator passes on changes to
	 * when it learns about file changes
	 */
	Map<String, Set<Integer>> fileListeners;

	/**
	 * Proposer only: (filename, opNum) -> last propNumber prepared
	 */
	private Map<Tuple<String, Integer>, Integer> lastProposalNumbersSent;

	/**
	 * Acceptors only: encapsulates persistent paxos data structures
	 */
	private PersistentPaxosState paxosState;

	/**
	 * Proposer only: (filename, opNum) -> number of acceptors that have
	 * responded with a promise
	 */
	private Map<Tuple<String, Integer>, Integer> promisesReceived;

	/**
	 * Proposer only: (filename, opNum) -> proposal w/ highest highest propNum
	 * promised
	 */
	private Map<Tuple<String, Integer>, Proposal> highestProposalReceived;

	/**
	 * 2PC coordinator: Maps filenames to transaction file lists, so for
	 * example, if a client is doing a transaction on files f1 and f2 the map
	 * would contain:
	 * 
	 * f1 -> f1, f2
	 * 
	 * f2 -> f1, f2
	 */
	private Map<String, Tuple<Integer, String[]>> fileTransactionMap;

	/**
	 * 2PC coordinator: A map from filenames in a transaction to the client
	 * transacting on these files.
	 */
	private Map<String, Integer> transactionAddressMap;

	/**
	 * 2PC coordinator: A set when starting up the 2PC coordinator. Should not
	 * be used anywhere except in receiveKnownFilenames().
	 * 
	 * TODO: what used for
	 */
	private Set<String> twoPCKnownFiles;

	/**
	 * 2PC
	 */
	private HashSet<String> pendingTries;

	/**
	 * Application data structures
	 */
	private FBCommands fbCommands;

	/**
	 * Simple hash function from filenames to addresses in [0,coordinatorCount)
	 */
	private static int hashFilename(String filename) {
		return Math.abs(filename.hashCode()) % coordinatorCount;
	}

	@Override
	public void start() {
		// Client
		this.commandGraph = new CommandGraph(this);
		this.relisten = new HashSet<String>();
		this.getCoordinatorOffset = new HashMap<String, Integer>();
		this.logFS = new LogFileSystem(this);
		this.transactingFiles = null;

		// Paxos
		this.acceptorsResponded = new HashMap<Tuple<String, Integer>, Integer>();
		this.fileListeners = new HashMap<String, Set<Integer>>();
		this.lastProposalNumbersSent = new HashMap<Tuple<String, Integer>, Integer>();
		this.paxosState = new PersistentPaxosState(this);
		this.promiseDenialResends = new HashSet<Tuple<String, Tuple<Integer, Integer>>>();
		this.promisesReceived = new HashMap<Tuple<String, Integer>, Integer>();
		this.highestProposalReceived = new HashMap<Tuple<String, Integer>, Proposal>();

		// 2PC
		fileTransactionMap = new HashMap<String, Tuple<Integer, String[]>>();
		transactionAddressMap = new HashMap<String, Integer>();
		if (addr == twoPCCoordinatorAddress) {
			for (int i = 0; i < coordinatorCount; i++) {
				RIOSend(i, MessageType.RequestAllFilenames);
			}
			addRequestFileTimeout();
		}
		twoPCKnownFiles = new HashSet<String>();
		pendingTries = new HashSet<String>();
		resolvedTransactionFiles = new HashSet<String>();

		// Application
		fbCommands = new FBCommands(this);

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

	public void acceptfriendParser(Tokenizer t) throws TransactionException {
		String friendName = t.next();
		fbCommands.acceptFriend(friendName).execute();
	}

	public void createuserParser(Tokenizer t) throws TransactionException {
		String username = t.next();
		// String password = t.next();
		fbCommands.createUser(username).execute();
	}

	public void loginParser(Tokenizer t) {
		String username = t.next();
		// String password = t.next();
		fbCommands.login(username).execute();
	}

	public void logoutParser(Tokenizer t) {
		fbCommands.logout().execute();
	}

	public void postParser(Tokenizer t) throws TransactionException {
		String content = t.rest();
		fbCommands.postMessage(content).execute();
	}

	public void updateParser(Tokenizer t) {
		fbCommands.readMessages().execute();
	}

	public void rejectfriendParser(Tokenizer t) {
		String friendName = t.next();
		fbCommands.rejectFriend(friendName).execute();
	}

	public void requestfriendParser(Tokenizer t) {
		String friendName = t.next();
		fbCommands.requestFriend(friendName).execute();
	}

	public void coordinatorsParser(Tokenizer t) {
		coordinatorCount = Integer.parseInt(t.next());
		printInfo("coordinatorCount set to " + coordinatorCount);
	}

	public void getlocalParser(Tokenizer t) {
		String filename = t.next();
		boolean exists = logFS.fileExists(filename);
		if (exists) {
			printInfo(filename + " has local content: "
					+ logFS.getFile(filename));
		} else {
			printInfo(filename + " does not exist locally");
		}
	}

	public void graphParser(Tokenizer t) throws IOException {
		// printVerbose("\n" + commandGraph.toString());
		// printVerbose("\n" + commandGraph.toDot());

		// TODO: close IO objects on failures...

		// write .dot
		FileWriter w = new FileWriter(realFilename(this.addr,
				"commandGraph.dot"));
		w.write(commandGraph.toDot());
		w.close();

		// run dot
		Process p = Runtime.getRuntime().exec(
				"dot -Tpng " + realFilename(this.addr, "commandGraph.dot"));

		BufferedInputStream in = new BufferedInputStream(p.getInputStream());
		BufferedOutputStream out = new BufferedOutputStream(
				new FileOutputStream(
						realFilename(this.addr, "commandGraph.png")));

		boolean done = false;
		while (!done) {
			try {
				if (p.exitValue() == 0) {
					bufferedWrite(in, out);
					printInfo("command graph written to "
							+ realFilename(this.addr, "commandGraph.png"));
				} else {
					printInfo("dot failed with exit value " + p.exitValue());
				}
				done = true;
			} catch (IllegalThreadStateException e) {
				bufferedWrite(in, out);
			}
		}
		out.close();
	}

	private void bufferedWrite(BufferedInputStream in, BufferedOutputStream out)
			throws IOException {
		int avail = in.available();
		if (avail > 0) {
			byte[] buffer = new byte[avail];
			int read = in.read(buffer);
			out.write(buffer, 0, read);
		}
	}

	/**
	 * framework...
	 */
	private static String realFilename(int nodeAddr, String filename) {
		return "storage/" + nodeAddr + "/" + filename;
	}

	public void packetsParser(Tokenizer t) {
		printInfo(this.packetsSent + " packets sent by this node");
		printInfo(TDFSNode.totalPacketsSent + " packets sent by all TDFSNodes");
	}

	public void perfileParser(Tokenizer t) {
		coordinatorsPerFile = Integer.parseInt(t.next());
		printInfo("coordinatorPerFile set to " + coordinatorsPerFile);

	}

	public void nodesParser(Tokenizer t) {
		maxTotalNodeCount = Integer.parseInt(t.next());
		printInfo("maxTotalNodeCount set to " + maxTotalNodeCount);

	}

	public void appendParser(Tokenizer t) {
		String filename = t.next();
		String contents = t.rest();
		append(filename, contents, buildAbortCommands()).execute();
	}

	public void createParser(Tokenizer t) {
		String filename = t.next();
		create(filename, buildAbortCommands()).execute();
	}

	public void deleteParser(Tokenizer t) {
		String filename = t.next();
		delete(filename, buildAbortCommands()).execute();
	}

	public void getParser(Tokenizer t) {
		String filename = t.next();
		get(filename, buildAbortCommands()).execute();
	}

	public void listenParser(Tokenizer t) {
		String filename = t.next();
		listen(filename).execute();
	}

	public void putParser(Tokenizer t) {
		String filename = t.next();
		String contents = t.rest();
		put(filename, contents, buildAbortCommands()).execute();
	}

	public void txcommitParser(Tokenizer t) throws TransactionException {
		txcommit().execute();
	}

	public void txstartParser(Tokenizer t) throws TransactionException {
		txstart(t.rest().split(commandDelim)).execute();
	}

	/**
	 * Returns null if not transacting (transactingFiles == null).
	 * 
	 * Throws a RuntimeException if transactingFiles == []
	 */
	protected List<Command> buildAbortCommands() {
		List<Command> abortCommands = null;
		if (transactingFiles != null) {
			if (transactingFiles.length == 0) {
				throw new RuntimeException("transactingFiles = []");
			}
			abortCommands = new ArrayList<Command>(transactingFiles.length);
			for (String tf : transactingFiles) {
				abortCommands
						.add(new TXCommand(transactingFiles, tf, this.addr) {
							@Override
							public void execute(TDFSNode node) throws Exception {
								if (node.logFS.checkLocked(filename) == node.addr) {
									createProposal(node, filename,
											new TXTryAbortLogEntry(filenames));
								}
								// else 2pc coordinator aborted me, do nothing
							}

							@Override
							public String getName() {
								return "TXAbort";
							}
						});
			}
		}
		return abortCommands;
	}

	public CommandNode append(String filename, String contents,
			List<Command> abortCommands) {
		CommandNode listen = listen(filename);
		commandGraph.addCommand(
				new WriteCommand(filename, contents, this.addr) {
					@Override
					public void execute(TDFSNode node) throws Exception {
						Integer lock = node.logFS.checkLocked(filename);
						if (lock == null || lock == nodeAddr) {
							if (node.logFS.fileExists(filename)) {
								createProposal(node, filename,
										new WriteLogEntry(contents, true));
							} else {
								throw new FileDoesNotExistException(filename);
							}
						}
					}

					@Override
					public String getName() {
						return "Write";
					}
				}, false, abortCommands);
		return listen;
	}

	public CommandNode create(String filename, List<Command> abortCommands) {
		CommandNode listen = listen(filename);
		commandGraph.addCommand(new FileCommand(filename, this.addr) {
			@Override
			public void execute(TDFSNode node) throws Exception {
				Integer lock = node.logFS.checkLocked(filename);
				if (lock == null || lock == nodeAddr) {
					if (!node.logFS.fileExists(filename)) {
						createProposal(node, filename, new CreateLogEntry());
					} else {
						throw new FileAlreadyExistsException(filename);
					}
				}
			}

			@Override
			public String getName() {
				return "Create";
			}
		}, false, abortCommands);
		return listen;
	}

	public CommandNode delete(String filename, List<Command> abortCommands) {
		CommandNode listen = listen(filename);
		commandGraph.addCommand(new FileCommand(filename, this.addr) {
			@Override
			public void execute(TDFSNode node) throws Exception {
				Integer lock = node.logFS.checkLocked(filename);
				if (lock == null || lock == nodeAddr) {
					if (node.logFS.fileExists(filename)) {
						createProposal(node, filename, new DeleteLogEntry());
					} else {
						throw new FileDoesNotExistException(filename);
					}
				}
			}

			@Override
			public String getName() {
				return "Delete";
			}
		}, false, abortCommands);
		return listen;
	}

	public CommandNode get(String filename, List<Command> abortCommands) {
		return append(filename, "", abortCommands);
	}

	public CommandNode listen(String filename) {
		return commandGraph.addCommand(new ListenCommand(filename, this.addr),
				false, null);
	}

	public CommandNode put(String filename, String contents,
			List<Command> abortCommands) {
		CommandNode listen = listen(filename);
		commandGraph.addCommand(
				new WriteCommand(filename, contents, this.addr) {
					@Override
					public void execute(TDFSNode node) throws Exception {
						Integer lock = node.logFS.checkLocked(filename);
						if (lock == null || lock == nodeAddr) {
							if (node.logFS.fileExists(filename)) {
								createProposal(node, filename,
										new WriteLogEntry(contents, false));
							} else {
								throw new FileDoesNotExistException(filename);
							}
						}
					}

					@Override
					public String getName() {
						return "Put";
					}
				}, false, abortCommands);
		return listen;
	}

	/**
	 * Assumes transactingFiles is not [].
	 * 
	 * Assumes all operations inside the commit are complete at time of
	 * execution.
	 */
	public CommandNode txcommit() throws TransactionException {
		if (transactingFiles != null) {
			CommandNode root = null;

			CommandNode listen = listen(transactingFiles[0]);
			if (root == null) {
				root = listen;
			}

			commandGraph.addCommand(new TXCommand(transactingFiles,
					transactingFiles[0], this.addr) {
				@Override
				public void execute(TDFSNode node) throws Exception {

					// Keeps a copy of the first transacting file in order to
					// use as a key
					if (node.resolvedTransactionFiles == null) {
						for (String file : filenames) {
							resolvedTransactionFiles.add(file);
						}
					}
					node.resolvedTransactionKey = filename;

					for (String filename : filenames) {
						Integer locked = node.logFS.checkLocked(filename);
						if (locked != null && locked == node.addr) {
							createProposal(node, filename,
									new TXTryCommitLogEntry(filenames));

						}
					}
					/*
					 * If I don't have the lock, then the 2PC coordinator
					 * committed or aborted this file
					 */
				}

				@Override
				public String getName() {
					return "TXCommit";
				}
			}, true, null);

			transactingFiles = null;
			return root;
		} else {
			throw new TransactionException("not in a transaction");
		}
	}

	public CommandNode txstart(String[] filenames) throws TransactionException {
		if (transactingFiles == null) {
			if (filenames.length == 0) {
				throw new TransactionException("empty filename list");
			}

			transactingFiles = filenames;

			// sort filenames
			List<String> sorted = new ArrayList<String>(transactingFiles.length);
			for (String filename : transactingFiles) {
				sorted.add(filename);
			}
			Collections.sort(sorted);
			transactingFiles = sorted.toArray(transactingFiles);

			CommandNode root = null;
			for (String filename : transactingFiles) {
				CommandNode listen = listen(filename);
				if (root == null) {
					root = listen;
				}
				commandGraph.addCommand(new TXCommand(transactingFiles,
						filename, this.addr) {
					@Override
					public void execute(TDFSNode node) throws Exception {
						if (node.logFS.checkLocked(filename) == null) {
							createProposal(node, filename, new TXStartLogEntry(
									filenames, node.addr));
						}
					}

					@Override
					public String getName() {
						return "TXStart";
					}
				}, true, null);
			}
			return root;
		} else {
			throw new TransactionException("already in transaction");
		}
	}

	/**
	 * Dynamically dispatches messages to receiveType(int, String|byte[])
	 * 
	 * Routes all messages to learn for 2PC coordinators
	 */
	@Override
	public void onRIOReceive(Integer from, MessageType type, byte[] msg) {

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
			Logger.verbose(this, "Node " + addr + ": not accepting proposal: "
					+ p.proposalNumber);
			Logger.verbose(
					this,
					"Node "
							+ addr
							+ ": highest proposal number promised: "
							+ paxosState.highestPromised(p.filename,
									p.operationNumber));
			Logger.verbose(this, "Node: " + addr + " not accepting proposal: "
					+ p.proposalNumber);
			Logger.verbose(
					this,
					"Node: "
							+ addr
							+ " highest proposal number promised: "
							+ paxosState.highestPromised(p.filename,
									p.operationNumber));
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
			Set<Integer> listeners = fileListeners.get(p.filename);
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
			ensureListening(filename);
		}
	}

	private void ensureListening(String filename) {
		Set<Integer> listeners = fileListeners.get(filename);
		if (listeners == null) {
			listeners = new HashSet<Integer>();
			listeners.add(this.addr);
			listeners.add(TDFSNode.twoPCCoordinatorAddress);
			fileListeners.put(filename, listeners);
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

		/**
		 * Before, not checking persistent storage whether something had been
		 * accepted for this operation number. If it is accepted, send them the
		 * old proposal so that they can re-propose it, assuming that this
		 * operation was lost somehow.
		 */
		// check persistent storage
		Proposal oldProposal = paxosState.highestAccepted(p.filename,
				p.operationNumber);
		if (oldProposal != null) { // Promise the old proposal
			while (oldProposal.proposalNumber <= largestProposalNumberPromised) {
				oldProposal.proposalNumber += maxTotalNodeCount;
			}
			oldProposal.originalProposal = oldProposal.proposalNumber;
			RIOSend(from, MessageType.Promise, oldProposal.pack());
			return;
		}

		// check local storage
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

			while (p.proposalNumber <= largestProposalNumberPromised) {
				// suboptimal
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
			if (p.originalProposal != -1)
				highestResponse.proposalNumber = p.originalProposal;
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

	/**
	 * Set of (filename, (opNum, propNum)) tracking promisesDenial responses
	 * send
	 * 
	 * TODO: cleanup on learns
	 */
	private Set<Tuple<String, Tuple<Integer, Integer>>> promiseDenialResends;

	public void receivePromiseDenial(int from, byte[] msg) {
		Proposal p = new Proposal(msg);

		Tuple<String, Tuple<Integer, Integer>> key = new Tuple<String, Tuple<Integer, Integer>>(
				p.filename, new Tuple<Integer, Integer>(p.operationNumber,
						p.proposalNumber));

		if (!promiseDenialResends.contains(key)) {
			promiseDenialResends.add(key);
			prepare(p);
		}
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
		List<Integer> coordinators = getCoordinators(filename);
		byte[] filenameBytes = Utility.stringToByteArray(filename);
		for (int next : coordinators) {
			RIOSend(next, MessageType.CreateGroup, filenameBytes);
		}

		ensureListening(filename);
		Set<Integer> listeners = fileListeners.get(filename);
		listeners.add(from);

		// Create the log if it doesn't exist locally
		if (!logFS.isListening(filename)) {
			logFS.createGroup(filename);
		}

		RIOSend(from, MessageType.AddedListener, logFS.packLog(filename));
	}

	/**
	 * Removes the requester from the list of file listeners
	 * 
	 * @param from
	 *            Who sent the request
	 * @param filename
	 *            The filename
	 */
	public void receiveRequestToLeave(int from, String filename) {
		if (fileListeners.containsKey(filename))
			fileListeners.get(filename).remove(from);
	}

	/**
	 * Request for all filenames this node knows about. Assumes that this node
	 * is a coordinator.
	 * 
	 * @param from
	 *            The address who sent the request
	 * @param filename
	 *            Should be blank
	 */
	public void receiveRequestAllFilenames(int from, String filename) {
		StringBuilder sb = new StringBuilder();
		String filenames = "";

		for (String file : fileListeners.keySet()) {
			sb.append(file + Proposal.packetDelimiter);
		}
		if (sb.length() > 0) {
			filenames = sb.toString().substring(0,
					sb.length() - Proposal.packetDelimiter.length());
		}
		byte[] packed = Utility.stringToByteArray(filenames);
		RIOSend(from, MessageType.KnownFilenames, packed);
	}

	/**
	 * Parses a list of known filenames, and if the 2PC coordinator does not
	 * know about any file in this list, it requests to listen to it
	 * 
	 * @param from
	 * @param filenames
	 */
	public void receiveKnownFilenames(int from, String filenames) {
		String[] files = filenames.split(Proposal.packetDelimiter);
		for (String file : files) {
			if (!twoPCKnownFiles.contains(file) && !file.equals("")) {
				byte[] packed = Utility.stringToByteArray(file);
				RIOSend(from, MessageType.RequestToListen, packed);
			}
		}
	}

	public void receiveAddedListener(int from, byte[] packedLog) {
		String filename = logFS.listen(packedLog);
		commandGraph.done(new CommandKey(filename, this.addr));
		relisten.remove(filename);

		if (addr == twoPCCoordinatorAddress && logFS.isListening(filename)) {
			Integer client = logFS.checkLocked(filename);
			Integer latestOpNumber = logFS.nextLogNumber(filename) - 1;
			LogEntry txTest = logFS.getLogEntry(filename, latestOpNumber);
			if (txTest instanceof TXTryCommitLogEntry
					|| txTest instanceof TXTryAbortLogEntry) {
				if (client != null) {
					blindAbortClientTx(filename, client);
				}
			}
		}

	}

	/**
	 * This proposal is now learned, so put it in the log
	 * 
	 * @param msg
	 *            The proposal, as a byte array
	 */
	public void receiveLearned(int from, byte[] msg) {
		// 2PC handler
		if (addr == twoPCCoordinatorAddress) {
			learn(from, msg);
			return;
		}
		Proposal p = new Proposal(msg);
		logFS.writeLogEntry(p.filename, p.operationNumber, p.operation);

		// tell the commandGraph to finish commands it might be executing
		if (p.operation instanceof TXStartLogEntry) {
			if (commandGraph.done(new CommandKey(p.filename, this.addr))) {
				printVerbose("finished: " + p.operation.toString());
			}
		} else if (p.operation instanceof TXCommitLogEntry) {
			resolvedTransactionFiles.remove(p.filename);
			if (resolvedTransactionFiles.size() == 0
					&& resolvedTransactionKey != null) {
				if (commandGraph.done(new CommandKey(resolvedTransactionKey,
						this.addr))) {
					this.resolvedTransactionKey = null;
					printVerbose("finished: " + p.operation.toString());
				}
			}
		} else if (p.operation instanceof TXAbortLogEntry) {
			// This is crap.
			// TODO: Wayne: Fix plx
			this.resolvedTransactionFiles.clear();
			this.resolvedTransactionKey = null;
			this.commandGraph = new CommandGraph(this);
		} else {

			/*
			 * txTryCommands aren't picked up here because they use a different
			 * subkey, so we pick them up above
			 */
			if (commandGraph.done(new CommandKey(p.filename, p.operationNumber,
					p.proposalNumber))) {
				printVerbose("finished: " + p.operation.toString());
			}
		}

		// Update the HTML page and write it out
		writeHTML();
	}

	/**
	 * Writes the HTML Data and saves it out to disk
	 */
	public void writeHTML() {
		if (fbCommands.currentUsername != null) {
			String username = fbCommands.currentUsername;
			String friendsData = "";
			String messageData = "";
			String requestData = "";
			String friendsFilename = FBCommands.getFriendsFilename(username);
			String requestsFilename = FBCommands.getRequestsFilename(username);
			String messageFilename = FBCommands.getMessagesFilename(username);

			if (logFS.isListening(friendsFilename)
					&& logFS.isListening(messageFilename)
					&& logFS.isListening(requestsFilename)) {
				friendsData = logFS.getFile(friendsFilename);
				messageData = logFS.getFile(messageFilename);
				requestData = logFS.getFile(requestsFilename);
			}

			HTML.write(friendsData, requestData, username, messageData,
					this.addr + ".html", this);
		} else {
			HTML.writeblank(this, this.addr + ".html");
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
		String[] txFiles = null;
		if (!logFS.isListening(p.filename)) {
			logFS.createGroup(p.filename);
		}
		logFS.writeLogEntry(p.filename, p.operationNumber, p.operation);
		Tuple<Integer, String[]> fileKey = fileTransactionMap.get(p.filename);
		if (fileKey != null) {
			txFiles = fileKey.second;
		}

		// check for transactions

		if (p.operation instanceof TXStartLogEntry) {
			TXStartLogEntry txCommand = (TXStartLogEntry) p.operation;

			// check for duplicate learns
			if (txFiles != null && txFiles.equals(txCommand.filenames))
				return;

			// start callback
			addTxTimeout(p.filename, txCommand.address, p.operationNumber);

			// add files
			String[] files = txCommand.filenames;

			for (String file : files) {
				Tuple<Integer, String[]> key = new Tuple<Integer, String[]>(
						p.operationNumber, files);
				fileTransactionMap.put(file, key);
				transactionAddressMap.put(file, txCommand.address);
			}
		}

		else if (p.operation instanceof TXTryCommitLogEntry) {
			// check for duplicate learns
			TXTryCommitLogEntry txCommand = (TXTryCommitLogEntry) p.operation;

			// check for duplicate learns
			if (txFiles == null)
				return;

			pendingTries.add(p.filename);

			boolean allContained = true;

			for (String file : txCommand.filenames) {
				if (!pendingTries.contains(file))
					allContained = false;
			}

			if (allContained) {
				createProposal(new TXCommitLogEntry(txCommand.filenames),
						txCommand.filenames);
				for (String file : txCommand.filenames) {
					pendingTries.remove(file);
					fileTransactionMap.put(file, null);
				}
			}

		}

		else if (p.operation instanceof TXTryAbortLogEntry) {
			String[] files = ((TXTryAbortLogEntry) p.operation).filenames;
			Integer client = transactionAddressMap.get(files[0]);

			Tuple<Integer, String[]> key = fileTransactionMap.get(p.filename);
			if (key != null) {
				abortClientTx(p.filename, client, key.first);
			}
			key = null;
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
	public void abortClientTx(String filename, Integer address,
			Integer originalOperationNumber) {

		String[] files = null;
		Integer currentOpNumber = null;
		Tuple<Integer, String[]> key = null;
		if (fileTransactionMap.containsKey(filename)) {
			key = fileTransactionMap.get(filename);
			if (key != null) {
				currentOpNumber = key.first;
				files = key.second;
			}
		}

		if (key == null || files == null
				|| currentOpNumber != originalOperationNumber)
			return; // Assume the client must have already aborted or committed
		// this tx

		if (logFS.isListening(filename)) {
			createProposal(new TXAbortLogEntry(files, address), files);
			for (String file : files) {
				fileTransactionMap.put(file, null);
			}
		}
	}

	/**
	 * Blindly aborts all tx for this client, assuming that state was lost
	 * somewhere and so it is necessary to abort all transactions this client is
	 * currently undertaking
	 * 
	 * @param filename
	 *            The filename to abort
	 * @param address
	 *            The address to abort
	 */
	public void blindAbortClientTx(String filename, Integer address) {
		Integer opNumber = null;
		if (fileTransactionMap.containsKey(filename)) {
			Tuple<Integer, String[]> key = fileTransactionMap.get(filename);
			opNumber = key.first;
		}
		if (opNumber != null) {
			abortClientTx(filename, address, opNumber);
		}
	}

	/**
	 * Adds a transaction timeout for the given client. If the client hasn't
	 * committed their transaction by the time the lease expires, then
	 * 
	 * @param client
	 */
	public void addTxTimeout(String filename, Integer address,
			Integer operationNo) {
		Method cbMethod = null;
		try {
			String[] params = { "java.lang.String", "java.lang.Integer",
					"java.lang.Integer" };
			cbMethod = Callback.getMethod("abortClientTx", this, params);
			cbMethod.setAccessible(true); // HACK
		} catch (Exception e) {
			printError(e);
			e.printStackTrace();
		}
		Object[] args = { filename, address, operationNo };
		Callback cb = new Callback(cbMethod, this, args);
		addTimeout(cb, TDFSNode.txTimeout);
	}

	public void addRequestFileTimeout() {
		Method cbMethod = null;
		try {
			String[] params = {};
			cbMethod = Callback.getMethod("requestTimeout", this, params);
			cbMethod.setAccessible(true); // HACK
		} catch (Exception e) {
			printError(e);
			e.printStackTrace();
		}
		Object[] args = {};
		Callback cb = new Callback(cbMethod, this, args);
		addTimeout(cb, 10);
	}

	public void requestTimeout() {
		if (addr == twoPCCoordinatorAddress) {
			for (int i = 0; i < coordinatorCount; i++) {
				RIOSend(i, MessageType.RequestAllFilenames);
			}
		}
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
			if (logFS.checkLocked(file) != null) {
				List<Integer> coordinators = getCoordinators(file);
				Proposal newProposal = new Proposal(operation, file,
						logFS.nextLogNumber(file), nextProposalNumber(file,
								logFS.nextLogNumber(file)));

				byte[] packed = newProposal.pack();
				for (Integer addr : coordinators) {
					RIOSend(addr, MessageType.Prepare, packed);
				}
			}
		}
	}
}
