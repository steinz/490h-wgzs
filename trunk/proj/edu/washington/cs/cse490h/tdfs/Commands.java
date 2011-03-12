package edu.washington.cs.cse490h.tdfs;

import java.util.Arrays;
import java.util.List;

import edu.washington.cs.cse490h.lib.Utility;

abstract class Command {
	/**
	 * TODO: HIGH: push these down the class hierarchy - aggregate commands like
	 * addFriend that are implemented in terms of other commands don't have a
	 * filename and don't nodeAddr (they must call done on their CommandNodes
	 * themselves after executing), and op/propNums are not needed for RPC or
	 * local operations (Get and Listen)
	 */
	protected final String filename;
	protected int nodeAddr;
	protected int operationNumber, proposalNumber;

	public Command(String filename, int nodeAddr) {
		this.filename = filename;
		this.nodeAddr = nodeAddr;
		this.operationNumber = -1;
		this.proposalNumber = -1;
	}

	/**
	 * TODO: HIGH: push down the class hierarchy- this is not needed for RPC or
	 * local operations (Get and Listen)
	 * 
	 * Creates the proposal to prepare and send to the list of coordinators
	 * 
	 * @param filename
	 *            The filename
	 * @param op
	 *            The operation
	 */
	public void createProposal(TDFSNode node, String filename, LogEntry op) {
		this.operationNumber = node.logFS.nextLogNumber(filename);
		this.proposalNumber = node.nextProposalNumber(filename,
				node.logFS.nextLogNumber(filename));
		Proposal proposal = new Proposal(op, filename, this.operationNumber,
				this.proposalNumber);
		node.prepare(proposal);
	}

	public abstract void execute(TDFSNode node) throws Exception;

	public abstract CommandKey getKey();

	/**
	 * a name for toString and toDot
	 */
	public String getName() {
		return "AnonymousCommand";
	}
	
	/**
	 * Convenience for subclass toStrings
	 */
	protected String toStringBase() {
		return this.getName() + " [filename=" + filename
				+ ", nodeAddr=" + nodeAddr + ", opNum=" + operationNumber
				+ ", propNum=" + proposalNumber;
	}

	@Override
	public String toString() {
		return toStringBase() + "]";
	}
}

abstract class FileCommand extends Command {

	public FileCommand(String filename, int nodeAddr) {
		super(filename, nodeAddr);
	}

	public CommandKey getKey() {
		return new CommandKey(this.filename, this.operationNumber,
				this.proposalNumber);
	}

	@Override
	public String getName() {
		return "AnonymousFileCommand";
	}
}

abstract class TXCommand extends Command {
	String[] filenames;

	public TXCommand(String[] filenames, String coordinatorFilename,
			int nodeAddr) {
		super(coordinatorFilename, nodeAddr);
		this.filenames = filenames;
	}

	public CommandKey getKey() {
		return new CommandKey(this.filename, this.nodeAddr);
	}
	
	@Override
	public String getName() {
		return "AnonymousTXCommand";
	}

	@Override
	public String toString() {
		return super.toStringBase() + ", filenames="
				+ Arrays.toString(filenames) + "]";
	}
}

abstract class WriteCommand extends FileCommand {
	String contents;

	public WriteCommand(String filename, String contents, int nodeAddr) {
		super(filename, nodeAddr);
		this.contents = contents;
	}

	@Override
	public String toString() {
		return super.toStringBase() + ", contents=" + contents;
	}
}

/**
 * Sends a RequestToListen to a coordinator of filename (itself if it is a
 * coordinator) unless already listening to filename and node.relisten does not
 * contain the filename.
 */
class ListenCommand extends FileCommand {
	public ListenCommand(String filename, int nodeAddr) {
		super(filename, nodeAddr);
	}

	@Override
	public void execute(TDFSNode node) {
		if (!node.relisten.contains(filename)
				&& node.logFS.isListening(filename)) {
			// no request to relisten and already listening, noop
			node.commandGraph.done(new CommandKey(filename, node.addr));
			return;
		}

		List<Integer> coordinators = node.getCoordinators(filename);
		if (coordinators.contains(node.addr)) {
			node.RIOSend(node.addr, MessageType.RequestToListen,
					Utility.stringToByteArray(filename));
		} else {
			node.RIOSend(node.getCoordinator(filename),
					MessageType.RequestToListen,
					Utility.stringToByteArray(filename));
		}
	}

	/**
	 * TODO: HIGH: Fix class hierarchy so this is inherited
	 */
	@Override
	public CommandKey getKey() {
		return new CommandKey(filename, nodeAddr);
	}
	
	@Override
	public String getName() {
		return "Listen";
	}
}

class AbortCommand extends TXCommand {
	public AbortCommand(String[] filenames, String coordinatorFilename,
			int nodeAddr) {
		super(filenames, coordinatorFilename, nodeAddr);
	}

	@Override
	public void execute(TDFSNode node) throws Exception {
		if (node.logFS.checkLocked(filename) == node.addr) {
			createProposal(node, filename,
					new TXTryAbortLogEntry(filenames));
		} else {
			// can happen if 2pc coordinator aborts you
			throw new TransactionException("lock not owned on "
					+ filename);
		}
	}
	
	@Override
	public String getName() {
		return "TXAbort";
	}
}