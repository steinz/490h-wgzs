import java.util.Random;
import java.util.concurrent.BlockingQueue;

public class BootStrappedClientStub {

	private Random rand = new Random(10);

	private BlockingQueue<String> commandQueue;
	private BlockingQueue<String> resultQueue;
	private BlockingQueue<String> getQueue;

	public BootStrappedClientStub(BlockingQueue<String> commandQueue,
			BlockingQueue<String> resultQueue, BlockingQueue<String> getQueue) {
		this.commandQueue = commandQueue;
		this.resultQueue = resultQueue;
		this.getQueue = getQueue;

		try {
			go();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void go() throws InterruptedException {
		while (true) {
			String line = commandQueue.take();
			String cmd = line.split(" ")[0].toLowerCase();

			if (rand.nextBoolean()) {
				// success
				if (cmd.equals("get")) {
					getQueue.put("file content");
				}
				resultQueue.put("");
			} else {
				// failure
				resultQueue.put("FAILED");
			}
		}
	}

}
