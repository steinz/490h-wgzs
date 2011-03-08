package edu.washington.cs.cse490h.tdfs;

import java.util.ArrayList;
import java.util.List;

import edu.washington.cs.cse490h.lib.Utility;

abstract class Command {
	protected final String filename;
	protected int nodeAddr, operationNumber, proposalNumber;

	public Command(String filename, int nodeAddr) {
		this.filename = filename;
		this.nodeAddr = nodeAddr;
		this.operationNumber = -1;
		this.proposalNumber = -1;
	}

	/**
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
}

abstract class FileCommand extends Command {

	public FileCommand(String filename, int nodeAddr) {
		super(filename, nodeAddr);
	}

	public CommandKey getKey() {
		return new CommandKey(this.filename, this.operationNumber,
				this.proposalNumber);
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

}

abstract class WriteCommand extends FileCommand {
	String contents;

	public WriteCommand(String filename, String contents, int nodeAddr) {
		super(filename, nodeAddr);
		this.contents = contents;
	}
}

class AppendCommand extends WriteCommand {
	public AppendCommand(String filename, String contents, int nodeAddr) {
		super(filename, contents, nodeAddr);
	}

	@Override
	public void execute(TDFSNode node) throws FileDoesNotExistException {
		if (node.logFS.fileExists(filename)) {
			createProposal(node, filename, new WriteLogEntry(contents, true));
		} else {
			throw new FileDoesNotExistException();
		}
	}
}

class CreateCommand extends FileCommand {
	public CreateCommand(String filename, int nodeAddr) {
		super(filename, nodeAddr);
	}

	@Override
	public void execute(TDFSNode node) throws FileAlreadyExistsException {
		if (!node.logFS.fileExists(filename))
			createProposal(node, filename, new CreateLogEntry());
		else
			throw new FileAlreadyExistsException();
	}
}

class DeleteCommand extends FileCommand {
	public DeleteCommand(String filename, int nodeAddr) {
		super(filename, nodeAddr);
	}

	@Override
	public void execute(TDFSNode node) throws FileAlreadyExistsException {
		if (node.logFS.fileExists(filename))
			createProposal(node, filename, new DeleteLogEntry());
		else
			throw new FileAlreadyExistsException();
	}
}

class GetCommand extends FileCommand {
	public GetCommand(String filename, int nodeAddr) {
		super(filename, nodeAddr);
	}

	@Override
	public void execute(TDFSNode node) throws FileDoesNotExistException {
		if (node.logFS.fileExists(filename)) {
			node.printInfo(node.logFS.getFile(filename));
			// HACK HACK HACK
			node.commandGraph.done(new CommandKey(filename, this.nodeAddr));
		} else {
			throw new FileDoesNotExistException();
		}
	}

	@Override
	public CommandKey getKey() {
		return new CommandKey(this.filename, this.nodeAddr);
	}
}

class ListenCommand extends FileCommand {
	public ListenCommand(String filename, int nodeAddr) {
		super(filename, nodeAddr);
	}

	@Override
	public void execute(TDFSNode node) {		
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
}

class PutCommand extends WriteCommand {
	public PutCommand(String filename, String contents, int nodeAddr) {
		super(filename, contents, nodeAddr);
	}

	@Override
	public void execute(TDFSNode node) throws FileDoesNotExistException {
		if (node.logFS.fileExists(filename)) {
			createProposal(node, filename, new WriteLogEntry(contents, false));
		} else {
			throw new FileDoesNotExistException();
		}
	}
}

class AbortCommand extends TXCommand {
	public AbortCommand(String[] filenames, String coordinatorFilename,
			int nodeAddr) {
		super(filenames, coordinatorFilename, nodeAddr);
	}

	@Override
	public void execute(TDFSNode node) {
		if (node.logFS.checkLocked(filename) == node.addr) {
			createProposal(node, filename, new TXTryAbortLogEntry(filenames));
		}
	}
}

class CommitCommand extends TXCommand {
	public CommitCommand(String[] filenames, String coordinatorFilename,
			int nodeAddr) {
		super(filenames, coordinatorFilename, nodeAddr);
	}

	@Override
	public void execute(TDFSNode node) {
		if (node.logFS.checkLocked(filename) == node.addr) {
			createProposal(node, filename, new TXTryCommitLogEntry(filenames));
		}
	}
}

class StartCommand extends TXCommand {
	public StartCommand(String[] filenames, String coordinatorFilename,
			int nodeAddr) {
		super(filenames, coordinatorFilename, nodeAddr);
	}

	@Override
	public void execute(TDFSNode node) {
		if (node.logFS.checkLocked(filename) == null) {
			createProposal(node, filename, new TXStartLogEntry(filenames,
					node.addr));
		}
	}
}
