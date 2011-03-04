package edu.washington.cs.cse490h.tdfs;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

abstract class Command {
	Command next;

	public abstract void execute(TDFSNode node, LogFS fs);
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
		node.checkIfListening(filename, new Write(contents, true));
	}
}

class CreateCommand extends FileCommand {
	public CreateCommand(String filename) {
		super(filename);
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		node.checkIfListening(filename, new Create());
	}
}

class DeleteCommand extends FileCommand {
	public DeleteCommand(String filename) {
		super(filename);
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		node.checkIfListening(filename, new Delete());
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

class PutCommand extends WriteCommand {
	public PutCommand(String filename, String contents) {
		super(filename, contents);
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		node.checkIfListening(filename, new Write(contents, false));
	}
}

class AbortCommand extends Command {

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		// TODO Auto-generated method stub
		
	}
}

class CommitCommand extends Command {

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		node.checkIfListening("", new TXCommit());
	}
}

class StartCommand extends Command {
	List<String> filenames;

	public StartCommand(List<String> filenames) {
		this.filenames = filenames;
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		Iterator<String> iter = filenames.iterator();
		while (iter.hasNext()){
			node.checkIfListening(iter.next(), new TXStart());
		}
		
	}
}


