package edu.washington.cs.cse490h.tdfs;

abstract class UserCommand extends Command {
	String username;

	public UserCommand(String username) {
		this.username = username;
	}
}

abstract class FriendCommand extends UserCommand {
	String friendUsername;

	public FriendCommand(String username, String friendUsername) {
		super(username);
		this.friendUsername = friendUsername;
	}
}

abstract class PasswordCommand extends UserCommand {
	String password;

	public PasswordCommand(String username, String password) {
		super(username);
		this.password = password;
	}
}

class NewuserCommand extends PasswordCommand {
	public NewuserCommand(String username, String password) {
		super(username, password);
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		// TODO Auto-generated method stub
		
	}
}

class LoginCommand extends PasswordCommand {
	public LoginCommand(String username, String password) {
		super(username, password);
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		// TODO Auto-generated method stub
		
	}
}

class LogoutCommand extends Command {

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		// TODO Auto-generated method stub
		
	}
}

class RequestCommand extends FriendCommand {
	public RequestCommand(String username, String friendUsername) {
		super(username, friendUsername);
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		// TODO Auto-generated method stub
		
	}
}

class AcceptCommand extends FriendCommand {
	public AcceptCommand(String username, String friendUsername) {
		super(username, friendUsername);
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		// TODO Auto-generated method stub
		
	}
}

class PostCommand extends UserCommand {
	String message;

	public PostCommand(String username, String message) {
		super(username);
		this.message = message;
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		// TODO Auto-generated method stub
		
	}
}

class ReadCommand extends UserCommand {
	public ReadCommand(String username) {
		super(username);
	}

	@Override
	public void execute(TDFSNode node, LogFS fs) {
		// TODO Auto-generated method stub
		
	}
}