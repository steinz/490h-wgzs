import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

public class CacheCoherenceTester extends PerfectInitializedClient {

	protected static Random random = new Random(7);

	/**
	 * Number of commands to perform
	 */
	protected static int commandCount = 100;

	@Override
	public void start() {
		super.start();
		printVerbose("initialized with addr: " + this.addr + " uuid: "
				+ this.ID);
	}

	@Override
	public void onCommand(String line) {
		if (line.equals("begin")) {
			doOp();
		} else {
			super.onCommand(line);
		}
	}

	@Override
	public void createHandler(StringTokenizer tokens, String line) {
		String filename = line.split(" ")[1];
		if (clientCacheStatus.containsKey(filename)
				&& clientCacheStatus.get(filename) != CacheStatuses.Invalid) {
			super.createHandler(tokens, line);
			doOp();
		} else {
			super.createHandler(tokens, line);
		}
	}

	@Override
	public void deleteHandler(StringTokenizer tokens, String line) {
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
	public void getHandler(StringTokenizer tokens, String line) {
		String filename = line.split(" ")[1];

		if (this.clientCacheStatus.containsKey((filename))
				&& clientCacheStatus.get(filename) != CacheStatuses.Invalid) {
			super.getHandler(tokens, line);
			doOp();
		} else {
			super.getHandler(tokens, line);
		}
	}

	@Override
	public void putHandler(StringTokenizer tokens, String line) {
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
	public void appendHandler(StringTokenizer tokens, String line) {
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
	protected void receiveWD(int from, String msgString) {
		super.receiveWD(from, msgString);
		doOp();
	}

	@Override
	protected void receiveRD(int from, String msgString) {
		super.receiveRD(from, msgString);
		doOp();
	}

	protected void doOp() {
		/*
		 * TODO: This needs to wait for a round for {R,W}C to get to the manager
		 * (maybe wait a few to be safe)
		 */

		if (commandCount < 1) {
			return;
		}

		commandCount--;

		int pickUpTo = 5;
		if (existingFiles.size() == 0) {
			pickUpTo = 1;
		}
		
		String filename;
		String cmd = "";
		switch (random.nextInt(pickUpTo)) {
		case 0:
			filename = randomNewFilename();
			cmd = "create " + filename;
			existingFiles.add(filename);
			break;
		case 1:
			cmd = "put " + randomExistingFilename() + " " + randomContent();
			break;
		case 2:
			cmd = "append " + randomExistingFilename() + " " + randomContent();
			break;
		case 3:
			cmd = "get " + randomExistingFilename();
			break;
		case 4:
			filename = randomExistingFilename();
			cmd = "delete " + filename;
			existingFiles.remove(filename);
			break;
		}

		Logger.info("Doing command: " + cmd);
		onCommand(cmd);

		if (commandCount > 0) {
			logSynopticEvent("NEW OPERATION");
		}
	}

	protected int name = 0;
	protected static List<String> existingFiles = new ArrayList<String>();
	
	protected String randomNewFilename() {
		return "f" + name++;
	}
	
	protected String randomExistingFilename() {
		return existingFiles.get(random.nextInt(existingFiles.size()));
	}

	protected String randomContent() {
		return random.nextInt(1000) + " ";
	}
}
