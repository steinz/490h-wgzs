package edu.washington.cs.cse490h.tdfs;

import java.util.ArrayList;
import java.util.List;

import edu.washington.cs.cse490h.tdfs.CommandGraph.CommandNode;

public class FBCommands {
	static String fileDelim = "\n";

	/**
	 * UserName of currently logged in user or null if not logged in
	 */
	protected String currentUsername;
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

		CommandNode root = node.txstart(filenames);
		List<Command> abortCommands = node.buildAbortCommands();

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

		String filename = getPasswordFilename(username);
		CommandNode root = node.get(filename, null);

		node.listen(getFriendsFilename(username));
		node.listen(getRequestsFilename(username));
		node.listen(getMessagesFilename(username));

		node.commandGraph.addCommand(new Command(filename, node.addr) {
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

		return root;
	}

	public CommandNode logout() {
		if (checkNotLoggedIn()) {
		}

		String fake = "logout";
		return node.commandGraph.addCommand(new Command(fake, node.addr) {

			@Override
			public void execute(TDFSNode node) throws Exception {
				currentUsername = null;
				node.commandGraph.done(new CommandKey(filename, node.addr));
			}

			@Override
			public CommandKey getKey() {
				return new CommandKey(filename, node.addr);
			}

		}, true, null);
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
		final String finalFriendName = friendName;

		CommandNode root = node.txstart(filenames);
		List<Command> abortCommands = node.buildAbortCommands();

		String filename = getRequestsFilename(currentUsername);
		node.get(filename, abortCommands);

		// TODO: HIGH: check request exists
		// TODO: HIGH: remove request from .requests
		node.commandGraph.addCommand(new FileCommand(filename, node.addr) {
			@Override
			public void execute(TDFSNode node) throws Exception {
				String requests = node.logFS.getFile(filename);
				if (requests.indexOf(finalFriendName) != -1) {
					String content = requests.replace(finalFriendName
							+ fileDelim, "");
					createProposal(node, filename, new WriteLogEntry(content,
							false));
				} else {
					throw new Exception("no request found from "
							+ finalFriendName);
				}
			}
		}, true, abortCommands);

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
		// TODO: HIGH: Verify CG state here (???)
		CommandNode root = node.get(filename, null);
		node.commandGraph.addCommand(new FileCommand(filename, node.addr) {
			@Override
			public void execute(TDFSNode node) throws Exception {
				String original = node.logFS.getFile(filename);
				String removed = original.replace(finalFriendName + fileDelim,
						"");
				// TODO: OPT: done if noop
				createProposal(node, filename,
						new WriteLogEntry(removed, false));
			}
		}, false, null);
		return root;
	}

	/**
	 * Doesn't support newlines in messages
	 * 
	 * @throws TransactionException
	 */
	public CommandNode postMessage(String message) throws TransactionException {
		if (checkNotLoggedIn()) {
			return node.commandGraph.noop();
		}
		String[] friends = node.logFS.getFile(
				getFriendsFilename(currentUsername)).split(fileDelim);
		String[] filenames = new String[friends.length + 1];
		for (int i = 0; i < friends.length; i++) {
			filenames[i] = getMessagesFilename(friends[i]);
		}
		filenames[friends.length] = getMessagesFilename(currentUsername);

		CommandNode root = node.txstart(filenames);
		List<Command> abortCommands = node.buildAbortCommands();

		for (String filename : filenames) {
			node.append(filename, currentUsername + ": " + message + fileDelim,
					abortCommands);
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
