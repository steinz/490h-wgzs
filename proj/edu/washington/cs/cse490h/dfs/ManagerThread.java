package edu.washington.cs.cse490h.dfs;

import java.io.IOException;

public class ManagerThread extends DFSThread {

	public ManagerThread(DFSNode node) throws IOException {
		super(node);
	}

	@Override
	public void run() {
		// loop on???
		while (!isInterrupted()) {
			try {
				// handle a network packet
				node.packetQueue.take();
			} catch (InterruptedException e) {
				// interrupted if node is restarting/terminating
				return;
			}
		}
	}
}
