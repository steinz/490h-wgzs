package edu.washington.cs.cse490h.app;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import edu.washington.cs.cse490h.dfs.DFSException;

/**
 * p2pFB app stub
 */
public class AcquaintanceCatalogue {

	private static DistributedFileSystem dfs;
	private static BufferedReader reader;
	private static boolean running;

	public static void main(String[] args) {
		dfs = new DistributedFileSystem();
		reader = new BufferedReader(new InputStreamReader(System.in));
		running = true;

		// login
		
		while (running) {
			try {
				repl();
			} catch (Exception e) {
				System.err.println("ERROR: " + e.toString());
			}
		}
	}

	private static void repl() throws Exception {
		print(sup("zuck"));
		print(viewFriends("zuck"));
		print("1 - view friends");
		print("2 - poke someone");
		String input = read();
		String output = eval(input);
		print(output);
	}

	private static String read() throws IOException {
		System.out.print(">");
		return reader.readLine();
	}

	private static String eval(String input) throws DFSException {
		String[] split = input.split(" ");

		if (split.length < 1) {
			return "no command";
		}
		String cmd = split[0].toLowerCase();

		if (cmd.equals("txabort")) {
			dfs.txAbort();
			return "aborted";
		} else if (cmd.equals("txcommit")) {
			dfs.txCommit();
			return "committed";
		} else if (cmd.equals("txstart")) {
			dfs.txStart();
			return "started";
		}

		if (split.length < 2) {
			return "no filename";
		}
		String filename = split[1];

		if (cmd.equals("create")) {
			dfs.create(filename);
			return "created";
		} else if (cmd.equals("delete")) {
			dfs.delete(filename);
			return "deleted";
		} else if (cmd.equals("get")) {
			return dfs.get(filename);
		}

		if (split.length < 3) {
			return "no contents";
		}
		String contents = input.substring(cmd.length() + filename.length() + 2);

		if (cmd.equals("append")) {
			dfs.append(filename, contents);
			return "appended";
		} else if (cmd.equals("put")) {
			dfs.put(filename, contents);
			return "put";
		}

		return "invalid command";
	}

	private static void addFriend(String you, String friend)
			throws DFSException {
		dfs.tryCreate(you + "_friend_list");
		dfs.tryCreate(friend + "_friend_list");

		dfs.txStart();
		dfs.append(you + "_friend_list", friend + "\n");
		dfs.append(friend + "_friend_list", you + "\n");
		dfs.txCommit();

	}

	private static void poke(String you, String friend) throws DFSException {
		dfs.tryCreate(friend + "_pokes");
		dfs.append(friend + "_pokes", you + "\n");
	}

	private static String viewFriends(String you) throws DFSException {
		return dfs.get(you + "_friend_list");
	}
	
	private static String sup(String you) throws DFSException {
		return dfs.get(you + "_pokes");

	}

	private static void print(String output) {
		System.out.println(output);
	}
}
