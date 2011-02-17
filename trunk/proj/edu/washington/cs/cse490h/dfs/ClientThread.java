package edu.washington.cs.cse490h.dfs;

import java.io.IOException;

public class ClientThread extends DFSThread {

	public ClientThread(DFSNode node) throws IOException {
		super(node);
	}

	@Override
	public void run() {
		// loop on???
		while (!isInterrupted()) {
			try {
				// handle a command
				String line = node.commandQueue.poll();
				if (line != null) {
					// handle command if there is one
				}

				// handle a network packet
				node.packetQueue.take();
			} catch (InterruptedException e) {
				// interrupted if node is restarting/terminating
				return;
			}
		}
	}
}
