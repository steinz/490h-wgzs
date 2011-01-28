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
		if (commandCount < 1) {
			return;
		}

		commandCount--;

		String filename = randomFileName();
		String content = randomContent();

		String cmd = "";
		switch (random.nextInt(5)) {
		case 0:
			cmd = "create " + filename;
			break;
		case 1:
			cmd = "put " + filename + " " + content;
			break;
		case 2:
			cmd = "append " + filename + " " + content;
			break;
		case 3:
			cmd = "get " + filename;
			break;
		case 4:
			cmd = "delete " + filename;
			break;
		}

		Logger.info("Doing command: " + cmd);
		onCommand(cmd);

		if (commandCount > 0) {
			logSynopticEvent("NEW OPERATION");
		}
	}

	protected static final String[] filenames = { "f1", "f2", "f3" };

	protected String randomFileName() {
		return filenames[random.nextInt(filenames.length)];
	}

	protected String randomContent() {
		return random.nextInt(1000) + " ";
	}
}
