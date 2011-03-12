package edu.washington.cs.cse490h.tdfs;

import java.io.BufferedInputStream;
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
	 * Username of the logged in user (user state)
	 */
	private String currentUsername = null;

	/**
	 * Client only: Map from filenames to most recent contents (populated by
	 * gets)
	 */
	protected Map<String, String> filestateCache;

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
	private String[] transactingFiles;

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
	private Map<String, String[]> fileTransactionMap;

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
	 * Simple hash function from filenames to addresses in [0,coordinatorCount)
	 */
	private static int hashFilename(String filename) {
		return filename.hashCode() % coordinatorCount;
	}

	@Override
	public void start() {
		// Client
		this.commandGraph = new CommandGraph(this);
		this.filestateCache = new HashMap<String, String>();
		this.relisten = new HashSet<String>();
		this.getCoordinatorOffset = new HashMap<String, Integer>();
		this.logFS = new LogFileSystem();
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
		fileTransactionMap = new HashMap<String, String[]>();
		transactionAddressMap = new HashMap<String, Integer>();
		if (addr == twoPCCoordinatorAddress) {
			for (int i = 0; i < coordinatorCount; i++) {
				RIOSend(i, MessageType.RequestAllFilenames);
			}
		}
		twoPCKnownFiles = new HashSet<String>();

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

	// public void acceptFriendParser(Tokenizer t) {
	// String friendName = t.next();
	//
	// String[] filenames = { currentUsername + ".friends",
	// friendName + ".friends" };
	// txstart(filenames);
	// // TODO: check that there's a pending request
	// append(filenames[0], friendName + commandDelim);
	// append(filenames[1], currentUsername + commandDelim);
	// txcommit();
	// }
	//
	// public void createUserParser(Tokenizer t) {
	// String username = t.next();
	// String password = t.next();
	//
	// String[] filenames = { username, username + ".friends",
	// username + ".requests", username + ".messages" };
	// txstart(filenames);
	// for (String filename : filenames) {
	// create(filename);
	// }
	// put(filenames[0], password);
	// txcommit();
	// }
	//
	// public void loginParser(Tokenizer t) {
	// String username = t.next();
	// String password = t.next();
	//
	// if (get(username).equals(password))
	// this.currentUsername = username;
	// }
	//
	// public void logoutParser(Tokenizer t) {
	// this.currentUsername = null;
	// }
	//
	// public void postMessageParser(Tokenizer t) {
	// String content = t.rest();
	// content = currentUsername + commandDelim + content.length()
	// + commandDelim + content;
	//
	// String filename = currentUsername + ".friends";
	// listen(filename);
	// /*
	// * TODO: HIGH: either get doesn't need to check listen or we don't need
	// * to check explicitly above
	// */
	// get(filename);
	// CommandNode get = commandGraph.addCommand(new GetCommand(filename,
	// this.addr), true, null);
	// CommandNode loaded = commandGraph.addCommand(new Command(filename,
	// this.addr) {
	// @Override
	// public CommandKey getKey() {
	// return new CommandKey(filename, addr);
	// }
	//
	// @Override
	// public void execute(TDFSNode node) throws Exception {
	// String[] friends = filestateCache.get(filename).split(
	// commandDelim);
	// }
	// }, false, null);
	//
	//
	//
	// // TODO: HIGH: how to delay this...
	// String[] friends = get(currentUsername + ".friends").split();
	// String[] messageFiles = new String[friends.length];
	// for (int i = 0; i < friends.length; i++) {
	// messageFiles[i] = friends[i] + ".messages";
	// }
	// txstart(friends);
	// for (String messageFile : messageFiles) {
	// append(messageFile, content);
	// }
	// txcommit();
	// }
	//
	// public void readMessagesParser(Tokenizer t) {
	// String messages = get(currentUsername + ".messages");
	// while (messages.length() > 0) {
	// Tokenizer m = new Tokenizer(messages, commandDelim);
	// String user = m.next();
	// int len = Integer.parseInt(m.next());
	// messages = m.rest();
	// String msg = messages.substring(0, len);
	// messages = messages.substring(len);
	// printVerbose(user + ": " + msg, true);
	// }
	// }
	//
	// public void requestFriendParser(Tokenizer t) {
	//
	// }
	//
	// public void showFriendsParser(Tokenizer t) {
	//
	// }

	public void coordinatorsParser(Tokenizer t) {
		coordinatorCount = Integer.parseInt(t.next());
		printInfo("coordinatorCount set to " + coordinatorCount);
	}

	public void graphParser(Tokenizer t) throws IOException,
			InterruptedException {
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
		FileOutputStream out = new FileOutputStream(realFilename(this.addr,
				"commandGraph.png"));

		boolean done = false;
		while (!done) {
			try {
				if (p.exitValue() == 0) {
					// TODO: buffer
					// write .png
					while (in.available() > 0) {
						out.write(in.read());
					}
					printInfo("command graph written to "
							+ realFilename(this.addr, "commandGraph.png"));
				} else {
					printInfo("dot failed with exit value " + p.exitValue());
				}
				done = true;
			} catch (IllegalThreadStateException e) {
				while (in.available() > 0) {
					out.write(in.read());
				}
			} 
		}
	}

	/**
	 * framework...
	 */
	private static String realFilename(int nodeAddr, String filename) {
		return "storage/" + nodeAddr + "/" + filename;
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

	public void txabortParser(Tokenizer t) throws TransactionException {
		txabort().execute();
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
	private List<Command> buildAbortCommands() {
		List<Command> abortCommands = null;
		if (transactingFiles != null) {
			if (transactingFiles.length == 0) {
				throw new RuntimeException("transactingFiles = []");
			}
			abortCommands = new ArrayList<Command>(transactingFiles.length);
			for (String tf : transactingFiles) {
				abortCommands.add(new AbortCommand(transactingFiles, tf,
						this.addr));
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
						if (node.logFS.fileExists(filename)) {
							createProposal(node, filename, new WriteLogEntry(
									contents, true));
						} else {
							throw new FileDoesNotExistException(filename);
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
				if (!node.logFS.fileExists(filename)) {
					createProposal(node, filename, new CreateLogEntry());
				} else {
					throw new FileAlreadyExistsException(filename);
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
				if (node.logFS.fileExists(filename)) {
					createProposal(node, filename, new DeleteLogEntry());
				} else {
					throw new FileDoesNotExistException(filename);
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
		CommandNode listen = listen(filename);
		commandGraph.addCommand(new FileCommand(filename, this.addr) {
			@Override
			public void execute(TDFSNode node) throws Exception {
				if (node.logFS.fileExists(filename)) {
					String content = node.logFS.getFile(filename);
					node.filestateCache.put(filename, content);
					node.commandGraph.done(new CommandKey(filename, -1, -1));
				} else {
					throw new FileDoesNotExistException(filename);
				}
			}

			@Override
			public String getName() {
				return "Get";
			}
			// TODO: HIGH: change getKey to return (filename, nodeAddr)?
		}, false, abortCommands);
		return listen;
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
						if (node.logFS.fileExists(filename)) {
							createProposal(node, filename, new WriteLogEntry(
									contents, false));
						} else {
							throw new FileDoesNotExistException(filename);
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
	 */
	public CommandNode txabort() throws TransactionException {
		if (transactingFiles != null) {
			CommandNode root = null;
			for (String filename : transactingFiles) {
				CommandNode listen = listen(filename);
				if (root == null) {
					root = listen;
				}
				commandGraph.addCommand(new AbortCommand(transactingFiles,
						filename, this.addr), true, null);
			}
			transactingFiles = null;
			return root;
		} else {
			throw new TransactionException("not in a transaction");
		}
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
			for (String filename : transactingFiles) {
				CommandNode listen = listen(filename);
				if (root == null) {
					root = listen;
				}
				commandGraph.addCommand(new TXCommand(transactingFiles,
						filename, this.addr) {
					@Override
					public void execute(TDFSNode node) throws Exception {
						if (node.logFS.checkLocked(filename) == node.addr) {
							createProposal(node, filename,
									new TXTryCommitLogEntry(filenames));
						} else {
							throw new TransactionException("lock not owned on "
									+ filename);
						}
					}

					@Override
					public String getName() {
						return "TXCommit";
					}
				}, true, null);
			}
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

			List<Command> abortCommands = buildAbortCommands();
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
				}, true, abortCommands);
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
		commandGraph.done(new CommandKey(filename, -1, -1));
		relisten.remove(filename);

		if (addr == twoPCCoordinatorAddress) {
			Integer client = logFS.checkLocked(filename);
			if (client != null) {
				abortClientTx(filename, client);
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
		if (p.operation instanceof TXAbortLogEntry
				|| p.operation instanceof TXCommitLogEntry
				|| p.operation instanceof TXStartLogEntry) {
			if (commandGraph.done(new CommandKey(p.filename, this.addr))) {
				printVerbose("finished: " + p.operation.toString());
			} else if (p.operation instanceof TXAbortLogEntry) {
				if (((TXAbortLogEntry) p.operation).address == this.addr) {
					printVerbose("abort injected, cancelling all pending operations");
					commandGraph = new CommandGraph(this);
					transactingFiles = null;
				}
			}
		} else {
			// txTryCommands aren't picked up here because they use a different
			// subkey
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
			addTxTimeout(p.filename, txCommand.address);

			// add files
			String[] files = txCommand.filenames;

			for (String file : files) {
				fileTransactionMap.put(file, files);
				transactionAddressMap.put(file, txCommand.address);
			}
		}

		else if (p.operation instanceof TXTryCommitLogEntry) {
			// check for duplicate learns
			TXTryCommitLogEntry txCommand = (TXTryCommitLogEntry) p.operation;

			// check for duplicate learns
			if (txFiles == null)
				return;

			String[] files = txCommand.filenames;
			createProposal(new TXCommitLogEntry(files), files);
			for (String file : txFiles) {
				fileTransactionMap.put(file, null);
			}

		}

		else if (p.operation instanceof TXTryAbortLogEntry) {
			String[] files = ((TXTryAbortLogEntry) p.operation).filenames;
			Integer client = transactionAddressMap.get(files[0]);
			abortClientTx(p.filename, client);
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
	public void abortClientTx(String filename, Integer address) {
		String[] files = fileTransactionMap.get(filename);
		if (files == null)
			return; // Assume the client must have already aborted or committed
		// this tx

		createProposal(new TXAbortLogEntry(files, address), files);
		for (String file : files) {
			fileTransactionMap.put(file, null);
		}
	}

	/**
	 * Adds a transaction timeout for the given client. If the client hasn't
	 * committed their transaction by the time the lease expires, then
	 * 
	 * @param client
	 */
	public void addTxTimeout(String filename, Integer address) {
		Method cbMethod = null;
		try {
			String[] params = { "java.lang.String", "java.lang.Integer" };
			cbMethod = Callback.getMethod("abortClientTx", this, params);
			cbMethod.setAccessible(true); // HACK
		} catch (Exception e) {
			printError(e);
			e.printStackTrace();
		}
		Object[] args = { filename, address };
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
