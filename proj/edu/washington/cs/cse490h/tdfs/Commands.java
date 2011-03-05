package edu.washington.cs.cse490h.tdfs;


abstract class Command {
	
	
	// TODO: HIGH: WAYNE: Check valid to execute
	public abstract void execute(TDFSNode node, LogFS fs);
	
	// TODO: HIGH: Retry?
	public abstract void retry(TDFSNode node, LogFS fs);
	
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
	
	public void retry(TDFSNode node, LogFS fs){
		execute(node, fs);
	}
}

class CreateCommand extends FileCommand {
	public CreateCommand(String filename) {
		super(filename);
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		createProposal(node, filename, new CreateLogEntry());
	}
	
	public void retry(TDFSNode node, LogFS fs){
		execute(node, fs);
	}
}

class DeleteCommand extends FileCommand {
	public DeleteCommand(String filename) {
		super(filename);
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		createProposal(node, filename, new DeleteLogEntry());
	}
	
	public void retry(TDFSNode node, LogFS fs){
		execute(node, fs);
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
	
	public void retry(TDFSNode node, LogFS fs){
		execute(node, fs);
	}
}

class ListenCommand extends FileCommand {
	public ListenCommand(String filename) {
		super(filename);
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		// TODO: HIGH: request to listen
		
	}
	
	public void retry(TDFSNode node, LogFS fs){
		execute(node, fs);
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
	
	public void retry(TDFSNode node, LogFS fs){
		execute(node, fs);
	}
}

class AbortCommand extends Command {

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		// TODO Auto-generated method stub
		
	}
	
	public void retry(TDFSNode node, LogFS fs){
		execute(node, fs);
	}
}

class CommitCommand extends Command {
	String[] filenames;
	
	public CommitCommand(String[] filenames){
		this.filenames = filenames;
	}
	
	@Override
	public void execute(TDFSNode node, LogFS fs) {
		createProposal(node, "", new TXCommitLogEntry(filenames));
	}
	
	public void retry(TDFSNode node, LogFS fs){
		execute(node, fs);
	}
}

class StartCommand extends Command {
	String filename;

	public StartCommand(String filename) {
		this.filename = filename;
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {		
		createProposal(node, filename, new TXStartLogEntry(filename));
	}
	
	public void retry(TDFSNode node, LogFS fs){
		execute(node, fs);
	}
}


