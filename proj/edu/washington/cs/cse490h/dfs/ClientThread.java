package edu.washington.cs.cse490h.dfs;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ClientThread extends DFSThread {

	/*
	 * TODO: HIGH: What is the elegant way to implement locking here?
	 */
	
	/*
	 * TODO: HIGH: Async or Sync FS ops from App layer?
	 */

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

					// EXAMPLE
					boolean remoteGet = true;
					if (remoteGet) {
						String filename = "test";
						BlockingQueue<DFSPacket> q = node.expectedPackets
								.get(filename);
						if (q == null) {
							// Safe sense Node won't touch this until after
							// send
							node.expectedPackets.put(filename,
									new LinkedBlockingQueue<DFSPacket>());
						}
						node.RIOSend(managerAddr, MessageType.RQ, filename);
						DFSPacket response = node.expectedPackets.get(filename)
								.take();
						if (response.type == MessageType.Error) {
							node.
						}
					}
					// EXAMPLE END
				}

				// handle a network packet
				DFSPacket packet = node.packetQueue.take();
			} catch (InterruptedException e) {
				// TODO: Verify this is safe for our IO
				// interrupted if node is restarting/terminating
				return;
			}
		}
	}
}
