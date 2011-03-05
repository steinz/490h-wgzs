package edu.washington.cs.cse490h.tdfs;

import edu.washington.cs.cse490h.lib.Utility;

abstract class Command {

	// TODO: HIGH: WAYNE: Check valid to execute
	public abstract void execute(TDFSNode node, LogFS fs);

	// TODO: HIGH: Retry?
	public void retry(TDFSNode node, LogFS fs) {
		execute(node, fs);
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
		int nextOperation = -1;
		try {
			nextOperation = node.logFS.nextLogNumber(filename);
		} catch (NotListeningException e) {
			Logger.error(node, e);
		}
		Proposal proposal = null;
		try {
			proposal = new Proposal(op, filename, nextOperation,
					node.nextProposalNumber(filename));
		} catch (NotListeningException e) {
			Logger.error(node, e);
		}
		node.prepare(node.addr, proposal.pack());
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
	public void execute(TDFSNode node, LogFS fs) {
		createProposal(node, filename, new WriteLogEntry(contents, true));
	}
}

class CreateCommand extends FileCommand {
	public CreateCommand(String filename) {
		super(filename);
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		if (!node.logFS.fileExists(filename))
			createProposal(node, filename, new CreateLogEntry());
		else
			Logger.error(node, "File already exists: " + filename);
	}
}

class DeleteCommand extends FileCommand {
	public DeleteCommand(String filename) {
		super(filename);
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		if (node.logFS.fileExists(filename))
			createProposal(node, filename, new DeleteLogEntry());
		else
			Logger.error(node, "File already exists: " + filename);
	}
}

class GetCommand extends FileCommand {
	public GetCommand(String filename) {
		super(filename);
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		// TODO Auto-generated method stub

	}
}

class ListenCommand extends FileCommand {
	public ListenCommand(String filename) {
		super(filename);
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		int coordinatorAddress = TDFSNode.hashFilename(filename);
		node.RIOSend(coordinatorAddress, MessageType.RequestToListen, Utility.stringToByteArray(filename));

	}
}

class PutCommand extends WriteCommand {
	public PutCommand(String filename, String contents) {
		super(filename, contents);
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		createProposal(node, filename, new WriteLogEntry(contents, false));
	}
}

class AbortCommand extends TXCommand {
	public AbortCommand(String[] filenames, String coordinatorFilename) {
		super(filenames, coordinatorFilename);
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		createProposal(node, coordinatorFilename, new TXTryAbortLogEntry(
				filenames));
	}
}

class CommitCommand extends TXCommand {
	public CommitCommand(String[] filenames, String coordinatorFilename) {
		super(filenames, coordinatorFilename);
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		createProposal(node, coordinatorFilename, new TXTryCommitLogEntry(
				filenames));
	}
}

class StartCommand extends TXCommand {
	public StartCommand(String[] filenames, String coordinatorFilename) {
		super(filenames, coordinatorFilename);
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		createProposal(node, coordinatorFilename,
				new TXStartLogEntry(filenames));
	}
}
