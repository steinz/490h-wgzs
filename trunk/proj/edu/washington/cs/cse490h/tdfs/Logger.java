package edu.washington.cs.cse490h.tdfs;

/**
 * CSE 490h
 * @author wayger, steinz
 */

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;

import edu.washington.cs.cse490h.lib.Node;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;

/**
 * Convenience logging methods
 */
class Logger {

	// TODO: Replace with java.util.logging.Logger

	/*
	 * TODO: LOW: It would be neat if the logger could take closures so that the
	 * strings would only be computed if logging was on. This could also help
	 * clean up long blocks of logging code elsewhere.
	 */

	/**
	 * The path to the server log
	 */
	private static final String LOG_FILE = "server_log.log";

	/**
	 * Print verbose messages. Feel free to change to false.
	 */
	private static final boolean printVerbose = false;
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

	private static int lineNumber = 0;

	public static void verbose(Node n, String str) {
		verbose(n, str, false);
	}

	public static void verbose(Node n, String str, boolean highlight) {
		if (printVerbose) {
			if (highlight) {
				infoPrintln(n, "\n===VERBOSE===");
			}
			infoPrintln(n, "|VERBOSE| " + str);
			if (highlight) {
				infoPrintln(n, "===VERBOSE===\n");
			}
		}
	}

	public static void info(Node n, String str) {
		if (printInfo) {
			infoPrintln(n, "   |INFO| " + str);
		}
	}

	public static void error(Node n, String str) {
		errorPrintln(n, "  |ERROR| " + str);
	}

	public static void error(Node n, Throwable e) {
		if (printError) {
			errorPrintln(n, "  |ERROR| " + e.toString());
			StackTraceElement[] trace = e.getStackTrace();
			for (StackTraceElement st : trace) {
				errorPrintln(n, "  |ERROR| " + st.toString());
			}
		}
	}

	private static void infoPrintln(Node n, String str) {
		infoStream.println((lineNumber++) + str);
		writeToLog(n, str);
	}

	private static void errorPrintln(Node n, String str) {
		errorStream.println(lineNumber + str);
		writeToLog(n, (lineNumber++) + str);
	}

	/**
	 * A quick and dirty method for wiping the server log at the beginning
	 */
	public static void eraseLog(Node n) {
		// node's log
		try {
			PersistentStorageWriter writer = n.getWriter(LOG_FILE, false);
			writer.write("");
		} catch (IOException e) {
			e.printStackTrace();
		}

		// global log
		try {
			BufferedWriter r = null;
			r = new BufferedWriter(new FileWriter(LOG_FILE, false));
			r.write("");
			r.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// TODO: LOW: keep the writer open between entries

	private static void writeToLog(Node n, String message) {
		// try {
		// PersistentStorageWriter writer = n.getWriter(LOG_FILE, true);
		// writer.write(message);
		// writer.newLine();
		// writer.close();
		// } catch (IOException e) {
		// e.printStackTrace();
		// }

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
		} catch (IOException e) {
			error(n, e);
		}
	}
}