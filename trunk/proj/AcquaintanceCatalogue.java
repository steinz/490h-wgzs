import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class AcquaintanceCatalogue {

	private static DistributedFileSystem dfs;
	private static BufferedReader reader;
	private static boolean running;

	public static void main(String[] args) {
		dfs = new DistributedFileSystem();
		reader = new BufferedReader(new InputStreamReader(System.in));
		running = true;

		while (running) {
			try {
				repl();
			} catch (Exception e) {
				System.err.println("ERROR: " + e.toString());
			}
		}
	}

	private static void repl() throws Exception {
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

	private static void print(String output) {
		System.out.println(output);
	}
}
