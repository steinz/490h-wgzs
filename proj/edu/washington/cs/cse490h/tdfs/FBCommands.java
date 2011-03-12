package edu.washington.cs.cse490h.tdfs;

import java.util.List;

import edu.washington.cs.cse490h.tdfs.CommandGraph.CommandNode;

public class FBCommands {
	private static String fileDelim = "\n";

	/**
	 * UserName of currently logged in user or null if not logged in
	 */
	private String currentUserName;
	private TDFSNode node;

	public static String getFriendsFilename(String username) {
		return username + ".friends";
	}

	public static String getMessagesFilename(String username) {
		return username + ".messages";
	}

	public static String getPasswordFilename(String username) {
		return username + ".password";
	}
	
	public static String getRequestsFilename(String username) {
		return username + ".requests";
	}

	public FBCommands(TDFSNode node) {
		this.node = node;
		this.currentUserName = null;
	}

	public boolean checkLoggedIn() {
		if (this.currentUserName != null) {
			node.printError("already logged in as: " + this.currentUserName);
		}
		return this.currentUserName != null;
	}

	public boolean checkNotLoggedIn() {
		if (this.currentUserName == null) {
			node.printError("not logged in");
		}
		return this.currentUserName == null;
	}

	public CommandNode login(String username) {
		if (checkLoggedIn()) {
			return null;
		}

		node.get(getPasswordFilename(currentUserName), null);
		
		// TODO: HIGH: passwords
		this.currentUserName = username;

		// TODO: HIGH: get stuff
	}

	public CommandNode logout() {
		if (checkNotLoggedIn()) {
			return null;
		}

		this.currentUserName = null;
		// TODO: High: Stop listening to all files associated with username
	}

	public CommandNode requestFriend(String friendName) {
		if (checkNotLoggedIn()) {
			return null;
		}

		node.append(getRequestsFilename(friendName), currentUserName + fileDelim, null);
	}

	public CommandNode acceptFriend(String friendName) throws TransactionException {
		if (checkNotLoggedIn()) {
			return null;
		}
		
		String[] filenames = { getFriendsFilename(currentUserName),
				getFriendsFilename(friendName) };
		List<Command> abortCommands = node.buildAbortCommands();

		node.txstart(filenames);
		node.get(getRequestsFilename(currentUserName), abortCommands);
		// TODO: HIGH: check request exists
		node.append(getFriendsFilename(currentUserName), friendName + fileDelim,
				abortCommands);
		node.append(getFriendsFilename(friendName), currentUserName + fileDelim,
				abortCommands);
		node.txcommit();
	}

	public CommandNode rejectFriend(String friendName) {
		if (checkNotLoggedIn()) {
			return null;
		}
		
		node.get(getRequestsFilename(currentUserName), null);
		// TODO: HIGH: remove friendName from .requests file

	}

	/**
	 * Doesn't support newlines in messages
	 */
	public CommandNode postMessage(String message) {
		if (checkNotLoggedIn()) {
			return null;
		}

		node.get(getFriendsFilename(currentUserName), null);
		// TODO: HIGH: post msg to friend's .message files
		node.append(getMessagesFilename(currentUserName), currentUserName + ": "
				+ message, null);
	}

	public CommandNode readMessages() {
		if (checkNotLoggedIn()) {
			return null;
		}

		node.get(getMessagesFilename(currentUserName), null);
	}
}
