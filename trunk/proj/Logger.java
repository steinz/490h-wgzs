import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Convenience logging methods
 */
public class Logger {

	/**
	 *
	 */
	private static final String LOG_FILE = "server_log.txt";

	/**
	 * Print verbose messages. Feel free to change to false.
	 */
	private static final boolean printVerbose = true;
	/**
	 * Print info messages Recommended to leave as true.
	 */
	private static final boolean printInfo = true;
	/**
	 * Print error messages. Recommended to leave as true.
	 */
	private static final boolean printError = true;

	/**
	 * Where to print info and verbose messages
	 */
	static PrintStream infoStream = System.out;
	/**
	 * Where to print error messages
	 */
	static PrintStream errorStream = System.err;

	public static void verbose(String str) {
		verbose(str, false);
	}

	// TODO: @Zach It would be neat if the logger could take methods instead of
	// strings so that the strings would only be computed if logging was on.
	// This would also clean up long logging block calls in code elsewhere.

	public static void verbose(String str, boolean highlight) {
		if (printVerbose) {
			if (highlight) {
				infoPrintln("\n===VERBOSE===");
			}
			infoPrintln(str);
			if (highlight) {
				infoPrintln("===VERBOSE===\n");
			}
		}
	}

	public static void info(String str) {
		if (printInfo) {
			infoPrintln(str);
		}
	}

	/**
	 * If there isn't an error code for your error yet, you can call this.
	 * Consider adding an error code instead.
	 * 
	 * @param str
	 */
	public static void error(String str) {
		error(ErrorCode.UnknownError, str);
	}

	public static void error(int errorCode, String str) {
		if (printError) {
			StringBuilder sb = new StringBuilder();
			sb.append(str);
			error(errorCode, sb);
		}
	}

	public static void error(int errorCode, StringBuilder sb) {
		if (printError) {
			sb.append(" [");
			sb.append(ErrorCode.lookup(errorCode));
			sb.append("]");
			errorPrintln(sb.toString());
		}
	}

	public static void error(Exception e) {
		if (printError) {
			errorPrintln(e.getMessage());
			StackTraceElement[] trace = e.getStackTrace();
			for (StackTraceElement st : trace) {
				errorPrintln(st.toString());
			}
		}
	}

	private static void infoPrintln(String str) {
		infoStream.println(str);
		writeToLog(str);
	}

	private static void errorPrintln(String str) {
		errorStream.println(str);
		writeToLog(str);
	}

	/**
	 * A quick and dirty method for wiping the server log at the beginning
	 */
	public static void eraseLog() {
		try {
			BufferedWriter r = null;
			r = new BufferedWriter(new FileWriter(LOG_FILE, false));
			r.write("");
			r.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void writeToLog(String message) {
		try {
			BufferedWriter r = null;

			// Create the log file if it doesn't exist already
			File outFile = new File(LOG_FILE);
			if (!outFile.exists()) {
				outFile.createNewFile();
			}

			r = new BufferedWriter(new FileWriter(LOG_FILE, true));

			r.write(message);
			r.newLine();

			r.flush();
			r.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}