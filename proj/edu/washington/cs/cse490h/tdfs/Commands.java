package edu.washington.cs.cse490h.tdfs;

import java.util.List;

import edu.washington.cs.cse490h.lib.Utility;

abstract class Command {

	public abstract void execute(TDFSNode node) throws Exception;

	/**
	 * Creates the proposal to prepare and send to the list of coordinators
	 * 
	 * @param filename
	 *            The filename
	 * @param op
	 *            The operation
	 */
	public void createProposal(TDFSNode node, String filename, LogEntry op) {
		int opNum = node.logFS.nextLogNumber(filename);
		int propNum = node.nextProposalNumber(filename, node.logFS.nextLogNumber(filename));
		Proposal proposal = new Proposal(op, filename, opNum, propNum);
		node.prepare(proposal);
	}
}

abstract class FileCommand extends Command {
	String filename;

	public FileCommand(String filename) {
		this.filename = filename;
	}
}

abstract class TXCommand extends Command {
	String[] filenames;
	String coordinatorFilename;

	public TXCommand(String[] filenames, String coordinatorFilename) {
		this.filenames = filenames;
		this.coordinatorFilename = coordinatorFilename;
	}
}

abstract class WriteCommand extends FileCommand {
	String contents;

	public WriteCommand(String filename, String contents) {
		super(filename);
		this.contents = contents;
	}
}

class AppendCommand extends WriteCommand {
	public AppendCommand(String filename, String contents) {
		super(filename, contents);
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
	public CreateCommand(String filename) {
		super(filename);
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
	public DeleteCommand(String filename) {
		super(filename);
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
	public GetCommand(String filename) {
		super(filename);
	}

	@Override
	public void execute(TDFSNode node) throws FileDoesNotExistException {
		if (node.logFS.fileExists(filename)) {
			node.printInfo(node.logFS.getFile(filename));
		} else {
			throw new FileDoesNotExistException();
		}
	}
}

class ListenCommand extends FileCommand {
	public ListenCommand(String filename) {
		super(filename);
	}

	@Override
	public void execute(TDFSNode node) {
		if (!node.logFS.isListening(filename)) {
			node.logFS.createGroup(filename);
		}

		List<Integer> coordinators = node.getCoordinators(filename);
		if (coordinators.contains(node.addr)) {
			for (int next : coordinators) {
				if (next != node.addr) {
					node.RIOSend(next, MessageType.CreateGroup, Utility
							.stringToByteArray(filename));
				}
			}
		} else {
			node.RIOSend(node.getCoordinator(filename),
					MessageType.RequestToListen, Utility
							.stringToByteArray(filename));
		}
	}
}

class PutCommand extends WriteCommand {
	public PutCommand(String filename, String contents) {
		super(filename, contents);
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
	public AbortCommand(String[] filenames, String coordinatorFilename) {
		super(filenames, coordinatorFilename);
	}

	@Override
	public void execute(TDFSNode node) {
		if (node.logFS.checkLocked(coordinatorFilename) == node.addr) {
			createProposal(node, coordinatorFilename, new TXTryAbortLogEntry(
					filenames));
		}
	}
}

class CommitCommand extends TXCommand {
	public CommitCommand(String[] filenames, String coordinatorFilename) {
		super(filenames, coordinatorFilename);
	}

	@Override
	public void execute(TDFSNode node) {
		if (node.logFS.checkLocked(coordinatorFilename) == node.addr) {
			createProposal(node, coordinatorFilename, new TXTryCommitLogEntry(
					filenames));
		}
	}
}

class StartCommand extends TXCommand {
	public StartCommand(String[] filenames, String coordinatorFilename) {
		super(filenames, coordinatorFilename);
	}

	@Override
	public void execute(TDFSNode node) {
		if (node.logFS.checkLocked(coordinatorFilename) == null) {
			createProposal(node, coordinatorFilename, new TXStartLogEntry(
					filenames, node.addr));
		}
	}
}
