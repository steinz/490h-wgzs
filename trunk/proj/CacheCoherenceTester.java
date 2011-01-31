/**
 * CSE 490h
 * @author wayger, steinz
 */

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import edu.washington.cs.cse490h.lib.Callback;

/**
 * Wraps all methods that might end w/o performing a send to perform a random
 * operation (and therefore send) once finished up to commandCount times (sends
 * must be made to keep the Simulator alive). Assumes the manager is node 0.
 */
public class CacheCoherenceTester extends PerfectInitializedClient {

	/**
	 * Static random number generator. TODO: Remove seed once class has been
	 * tested.
	 */
	protected static Random random = new Random(7);

	/**
	 * Number of commands to perform
	 */
	protected static int commandCount = 1000;

	/**
	 * A list of the non-manager clients in the swarm
	 */
	protected static List<CacheCoherenceTester> clients = new ArrayList<CacheCoherenceTester>();

	/**
	 * Add yourself to the client list if you're not the manager
	 */
	@Override
	public void start() {
		super.start();
		if (this.addr != 0) {
			clients.add(this);
		}
	}

	/**
	 * Wraps onCommand to handle the new command "begin" which should be issued
	 * to ONLY ONE client to start an op chain
	 */
	@Override
	public void onCommand(String line) {
		if (line.equals("begin")) {
			doOp();
		} else {
			super.onCommand(line);
		}
	}

	/***************************************************************************
	 * Begin wrapper for methods that can finish a high level op. Start a new op
	 * after finishing this one. The Handler logic checks need to match super's
	 * checks. However, we can ignore client locks since we do ops one at a time
	 **************************************************************************/

	@Override
	public void createHandler(StringTokenizer tokens, String line)
			throws IOException, UnknownManagerException {
		String filename = line.split(" ")[1];
		if (clientCacheStatus.containsKey(filename)
				&& clientCacheStatus.get(filename) == CacheStatuses.ReadWrite) {
			super.createHandler(tokens, line);
			doOp();
		} else {
			super.createHandler(tokens, line);
		}
	}

	@Override
	public void deleteHandler(StringTokenizer tokens, String line)
			throws IOException, UnknownManagerException {
		String filename = line.split(" ")[1];

		if (this.clientCacheStatus.containsKey((filename))
				&& clientCacheStatus.get(filename) == CacheStatuses.ReadWrite) {
			super.deleteHandler(tokens, line);
			doOp();
		} else {
			super.deleteHandler(tokens, line);
		}
	}

	@Override
	public void getHandler(StringTokenizer tokens, String line)
			throws IOException, UnknownManagerException {
		String filename = line.split(" ")[1];

		if (this.clientCacheStatus.containsKey((filename))) {
			super.getHandler(tokens, line);
			doOp();
		} else {
			super.getHandler(tokens, line);
		}
	}

	@Override
	public void putHandler(StringTokenizer tokens, String line)
			throws IOException, UnknownManagerException {
		String filename = line.split(" ")[1];

		if (this.clientCacheStatus.containsKey((filename))
				&& clientCacheStatus.get(filename) == CacheStatuses.ReadWrite) {
			super.putHandler(tokens, line);
			doOp();
		} else {
			super.putHandler(tokens, line);
		}
	}

	@Override
	public void appendHandler(StringTokenizer tokens, String line)
			throws IOException, UnknownManagerException {
		String filename = line.split(" ")[1];

		if (this.clientCacheStatus.containsKey((filename))
				&& clientCacheStatus.get(filename) == CacheStatuses.ReadWrite) {
			super.appendHandler(tokens, line);
			doOp();
		} else {
			super.appendHandler(tokens, line);
		}
	}

	@Override
	protected void receiveWD(int from, String msgString) throws IOException,
			UnknownManagerException, IllegalConcurrentRequestException,
			MissingPendingRequestException {
		super.receiveWD(from, msgString);
		if (!isManager) {
			doOp();
		}
	}

	@Override
	protected void receiveRD(int from, String msgString) throws IOException,
			UnknownManagerException {
		super.receiveRD(from, msgString);
		if (!isManager) {
			doOp();
		}
	}

	// TODO: Are receive{Error, Successful} client only?

	@Override
	protected void receiveError(Integer from, String msgString)
			throws NotClientException {
		super.receiveError(from, msgString);
		if (!isManager) {
			doOp();
		}
	}

	@Override
	protected void receiveSuccessful(int from, String msgString)
			throws Exception {
		super.receiveSuccessful(from, msgString);
		if (!isManager) {
			doOp();
		}
	}

	/**************************************************************
	 * End wrappers
	 **************************************************************/

	protected static final int DO_OP_WAIT = 10;

	/**
	 * Convenience method - call this for now
	 * 
	 * TODO: switch flag to turn on invalid operations once the protocol is
	 * robust
	 */
	protected void doOp() {
		Logger.verbose("REGISTERING NEW OPERATION " + commandCount);

		Method doOpMethod = null;
		boolean arg = true;
		CacheCoherenceTester client = clients.get(random
				.nextInt(clients.size()));

		try {
			doOpMethod = Callback.getMethod("doOp", client,
					new String[] { "java.lang.Boolean" });
		} catch (SecurityException e) {
			printError(e);
		} catch (ClassNotFoundException e) {
			printError(e);
		} catch (NoSuchMethodException e) {
			printError(e);
		}

		client.addTimeout(
				new Callback(doOpMethod, client, new Object[] { arg }),
				DO_OP_WAIT);
	}

	/**
	 * Does a random operation - operations can fail if !onlyValid.
	 * 
	 * This is only public so that it is visible to the Callback Utility - this
	 * shouldn't be called externally.
	 */
	// used as callback
	public void doOp(Boolean onlyValidW) {

		boolean onlyValid = onlyValidW.booleanValue();

		if (commandCount < 1) {
			// done desired number of commands
			return;
		}

		// print something to the log for Synoptic to divide traces on w/ -s
		logSynopticEvent("NEW OPERATION");
		Logger.verbose("NEW OPERATION " + commandCount);

		// decrement commands left counter
		commandCount--;

		int pickUpTo = 5;
		if (onlyValid && existingFiles.size() == 0) {
			// must create
			pickUpTo = 1;
		}

		String filename;
		String cmd = "";
		String cmdName = "";

		switch (random.nextInt(pickUpTo)) {
		case 0:
			filename = getFilename(onlyValid, true);
			cmd = "create " + filename;
			cmdName = "CREATE";
			existingFiles.add(filename);
			break;
		case 1:
			filename = getFilename(onlyValid, false);
			cmd = "put " + filename + " " + randomContent();
			cmdName = "PUT";
			break;
		case 2:
			filename = getFilename(onlyValid, false);
			cmd = "append " + filename + " " + randomContent();
			cmdName = "APPEND";
			break;
		case 3:
			filename = getFilename(onlyValid, false);
			cmd = "get " + filename;
			cmdName = "GET";
			break;
		case 4:
			filename = getFilename(onlyValid, false);
			cmd = "delete " + filename;
			cmdName = "DELETE";
			existingFiles.remove(filename);
			break;
		}

		// log command
		printInfo("doing command: " + cmd);

		// a fake root for Synoptic
		//logSynopticEvent("COMMAND");
		// first child should always be command name
		logSynopticEvent(cmdName);

		// run the command
		onCommand(cmd);
	}

	/**
	 * Wrapper around different filename getters / creators
	 */
	protected String getFilename(boolean onlyValid, boolean newFile) {
		if (onlyValid) {
			if (newFile) {
				return newFilename();
			} else {
				return existingFilename();
			}
		} else {
			return randomFilename();
		}
	}

	/**
	 * Counter of files created for onlyValid
	 */
	protected static int name = 0;
	/**
	 * List of existing files for onlyValid
	 */
	protected static List<String> existingFiles = new ArrayList<String>();

	/**
	 * Returns a new filename for onlyValid
	 */
	protected String newFilename() {
		return "f" + name++;
	}

	/**
	 * Returns an existing filename for onlyValid
	 * 
	 * @return
	 */
	protected String existingFilename() {
		return existingFiles.get(random.nextInt(existingFiles.size()));
	}

	/**
	 * Max number of filenames to use when !onlyValid
	 */
	protected final int MAX_FILES = 10;

	/**
	 * Return a random filename for !onlyValid
	 */
	protected String randomFilename() {
		return "f" + random.nextInt(MAX_FILES);
	}

	/**
	 * Return a random integer followed by a space as a String to use as random
	 * content
	 */
	protected String randomContent() {
		return random.nextInt(1000) + "";
	}
}
