package edu.washington.cs.cse490h.app;

import java.util.Collections;
import java.util.StringTokenizer;

import edu.washington.cs.cse490h.tdfs.FileAlreadyExistsException;
import edu.washington.cs.cse490h.tdfs.FileDoesNotExistException;

public class TDFSApp {

	private static String lineSep = System.getProperty("line.separator");

	private static TDFS fs;

	private static String activeUser;

	private static void main(String[] args) {
		// TODO: repl
	}

	private static void display(String msg) {
		System.out.println(msg);
	}

	private static void createUser(String username, String password) {
		try {
			fs.create(username);
			fs.put(username, password);
			fs.create(username + "_friend_requests");
			fs.create(username + "_friends");
			fs.create(username + "_messages");
		} catch (FileAlreadyExistsException e) {
			display("username already taken");
		} catch (FileDoesNotExistException e) {
			throw new RuntimeException(e);
		}
	}

	private static void login(String username, String password) {
		try {
			String expectedPassword = fs.get(username);
			if (expectedPassword.equals(password)) {
				activeUser = username;
				display("logged in as " + username);
			} else {
				display("incorrect password");
			}
		} catch (FileDoesNotExistException e) {
			display("incorrect username");
		}
	}

	private static void logout() {
		if (activeUser != null) {
			activeUser = null;
			display("logged out");
		} else {
			display("not logged in");
		}
	}

	private static void requestFriend(String friendUsername) {
		if (notLoggedIn()) {
			return;
		}
		if (fs.exists(friendUsername)) {
			try {
				fs.append(friendUsername + "_friend_requests", activeUser
						+ lineSep);
			} catch (FileDoesNotExistException e) {
				throw new RuntimeException(e);
			}
			display("requested friend " + friendUsername);
		} else {
			display(friendUsername + " not found");
		}
	}

	private static void acceptFriend(String friendUsername) {
		if (notLoggedIn()) {
			return;
		}
		String[] fnames = { activeUser + "_friends",
				friendUsername + "_friends" };
		try {
			fs.txStart(fnames);
			fs.append(activeUser + "_friends", friendUsername + lineSep);
			fs.append(friendUsername + "_friends", activeUser + lineSep);
			fs.txCommit();
		} catch (AlreadyInTransactionException e) {
			throw new RuntimeException(e);
		} catch (NotInTransactionException e) {
			throw new RuntimeException(e);
		} catch FileDoesNotExistException e) {
			throw new RuntimeException(e);
		}
	}

	private static void postMessage(String msg) {
		if (notLoggedIn()) {
			return;
		}
		String[] friends;
		try {
			friends = fs.get(activeUser + "_friends").split(lineSep);
		} catch (FileDoesNotExistException e) {
			throw new RuntimeException(e);
		}

		msg = msg + lineSep;
		int length = msg.length();
		msg = activeUser + lineSep + length + lineSep + msg;

		String[] fnames = new String[friends.length];
		for (int i = 0; i < friends.length; i++) {
			fnames[i] = friends[i] + "_messages";
		}
		for (String fname : fnames) {
			try {
				fs.append(fname, msg);
			} catch (FileDoesNotExistException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static void readMessages() {
		if (notLoggedIn()) {
			return;
		}
		try {
			String msgs = fs.get(activeUser) + "_messages";
			while (msgs.length() > 0) {
				StringTokenizer t = new StringTokenizer(msgs, lineSep, true);
				String from = t.nextToken();
				String len = t.nextToken();
				int length = Integer.parseInt(len);
				int offset = from.length() + len.length();
				String msg = msgs.substring(offset, offset + length);
				msg.replace(lineSep, lineSep + "  ");
				display(from + msg);
				msgs = msgs.substring(offset + length);
			}
		} catch (FileDoesNotExistException e) {
			throw new RuntimeException(e);
		}
	}

	private static void showFriends() {
		if (notLoggedIn()) {
			return;
		}
		try {
			String friendList = fs.get(activeUser) + "_friends";
			display(friendList);
		} catch (FileDoesNotExistException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Helper that checks activeUser isn't null and prints a message if it is
	 */
	private static boolean notLoggedIn() {
		if (activeUser == null) {
			display("log in to do that");
			return true;
		} else {
			return false;
		}
	}
}
