package edu.washington.cs.cse490h.tdfs;

import java.util.List;

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
		// TODO Auto-generated method stub
		
	}
}

class CreateCommand extends FileCommand {
	public CreateCommand(String filename) {
		super(filename);
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		// TODO Auto-generated method stub
		
	}
}

class DeleteCommand extends FileCommand {
	public DeleteCommand(String filename) {
		super(filename);
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		// TODO Auto-generated method stub
		
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
		// TODO Auto-generated method stub
		
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
		// TODO Auto-generated method stub
		
	}
}

class StartCommand extends Command {
	List<String> filenames;

	public StartCommand(List<String> filenames) {
		this.filenames = filenames;
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		// TODO Auto-generated method stub
		
	}
}
