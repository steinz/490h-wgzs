package edu.washington.cs.cse490h.tdfs;

import java.util.ArrayList;
import java.util.List;

import edu.washington.cs.cse490h.tdfs.CommandGraph.CommandNode;

public class FBCommands {
	private static String fileDelim = "\n";

	/**
	 * UserName of currently logged in user or null if not logged in
	 */
	private String currentUsername;
	private TDFSNode node;

	public static String getFriendsFilename(String username) {
		return username + ".friends";
	}

	public static String getMessagesFilename(String username) {
		return username + ".messages";
	}

	/**
	 * login etc depends on this just being the username
	 */
	public static String getPasswordFilename(String username) {
		return username;
	}

	public static String getRequestsFilename(String username) {
		return username + ".requests";
	}

	public FBCommands(TDFSNode node) {
		this.node = node;
		this.currentUsername = null;
	}

	public boolean checkLoggedIn() {
		if (this.currentUsername != null) {
			node.printError("already logged in as: " + this.currentUsername);
		}
		return this.currentUsername != null;
	}

	public boolean checkNotLoggedIn() {
		if (this.currentUsername == null) {
			node.printError("not logged in");
		}
		return this.currentUsername == null;
	}

	public CommandNode createUser(String username) throws TransactionException {
		String[] filenames = { getPasswordFilename(username),
				getFriendsFilename(username), getRequestsFilename(username),
				getMessagesFilename(username) };

		List<Command> abortCommands = node.buildAbortCommands();
		CommandNode root = node.txstart(filenames);
		for (String filename : filenames) {
			node.create(filename, abortCommands);
		}
		node.txcommit();
		return root;
	}

	public CommandNode login(String username) {
		if (checkLoggedIn()) {
			return node.commandGraph.noop();
		}

		String filename = getPasswordFilename(currentUsername);
		CommandNode root = node.get(filename, null);
		CommandNode loaded = node.commandGraph.addCommand(new Command(filename,
				node.addr) {
			@Override
			public CommandKey getKey() {
				return new CommandKey(filename, node.addr);
			}

			@Override
			public void execute(TDFSNode node) throws Exception {
				currentUsername = filename;
				node.commandGraph.done(new CommandKey(filename, node.addr));
			}
		}, true, null);
		root.addChild(loaded);

		loaded.addChild(node.listen(getFriendsFilename(currentUsername)));
		loaded.addChild(node.listen(getRequestsFilename(currentUsername)));
		loaded.addChild(node.listen(getMessagesFilename(currentUsername)));

		return root;
	}

	public CommandNode logout() {
		if (checkNotLoggedIn()) {
		}

		this.currentUsername = null;
		// TODO: High: Stop listening to all files associated with username
		return node.commandGraph.noop();
	}

	public CommandNode requestFriend(String friendName) {
		if (checkNotLoggedIn()) {
			return node.commandGraph.noop();
		}

		return node.append(getRequestsFilename(friendName), currentUsername
				+ fileDelim, null);
	}

	public CommandNode acceptFriend(String friendName)
			throws TransactionException {
		if (checkNotLoggedIn()) {
			return node.commandGraph.noop();
		}

		String[] filenames = { getFriendsFilename(currentUsername),
				getFriendsFilename(friendName) };
		List<Command> abortCommands = node.buildAbortCommands();

		CommandNode root = node.txstart(filenames);
		node.get(getRequestsFilename(currentUsername), abortCommands);
		// TODO: HIGH: check request exists
		node.append(getFriendsFilename(currentUsername),
				friendName + fileDelim, abortCommands);
		node.append(getFriendsFilename(friendName),
				currentUsername + fileDelim, abortCommands);
		node.txcommit();
		return root;
	}

	public CommandNode rejectFriend(String friendName) {
		if (checkNotLoggedIn()) {
			return node.commandGraph.noop();
		}

		final String finalFriendName = friendName;

		String filename = getRequestsFilename(currentUsername);
		CommandNode root = node.get(filename, null);
		root.addChild(node.commandGraph.addCommand(new FileCommand(filename,
				node.addr) {
			@Override
			public void execute(TDFSNode node) throws Exception {
				// GROSS!!
				String[] requests = node.filestateCache.get(filename).trim()
						.split(fileDelim);
				List<String> l = new ArrayList<String>();
				for (String request : requests) {
					l.add(request);
				}
				l.remove(finalFriendName);
				StringBuilder removed = new StringBuilder();
				for (String s : l) {
					removed.append(s + fileDelim);
				}
				createProposal(node, filename, new WriteLogEntry(removed
						.toString().trim(), false));
			}
		}, false, null));
		return root;
	}

	/**
	 * Doesn't support newlines in messages
	 * @throws TransactionException 
	 */
	public CommandNode postMessage(String message) throws TransactionException {
		if (checkNotLoggedIn()) {
			return node.commandGraph.noop();
		}
		String[] friends = node.filestateCache.get(
				getFriendsFilename(currentUsername)).split(fileDelim);
		String[] filenames = new String[friends.length + 1];
		for (int i = 0; i < friends.length; i++) {
			filenames[i] = getMessagesFilename(friends[i]);
		}
		filenames[friends.length] = getMessagesFilename(currentUsername);
		List<Command> abortCommands = node.buildAbortCommands();
		
		CommandNode root = node.txstart(filenames);
		for (String filename : filenames) {
			node.append(filename, currentUsername + ": " + message, abortCommands);
		}
		node.txcommit();
		return root;
	}

	public CommandNode readMessages() {
		if (checkNotLoggedIn()) {
			return node.commandGraph.noop();
		}

		return node.get(getMessagesFilename(currentUsername), null);
	}
}
