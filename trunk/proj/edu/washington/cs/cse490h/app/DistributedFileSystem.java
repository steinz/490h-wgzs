package edu.washington.cs.cse490h.app;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

import edu.washington.cs.cse490h.dfs.DFSException;

/**
 * CSE 490h
 * 
 * @author wayger, steinz
 */

/**
 * middleware between DFSNode and application
 */
public class DistributedFileSystem {

	private class ClientThread extends Thread {
		private BlockingQueue<String> commandQueue;
		private BlockingQueue<String> resultQueue;
		private BlockingQueue<String> getQueue;
		// Client client;
		BootStrappedClientStub client;

		public ClientThread(BlockingQueue<String> commandQueue,
				BlockingQueue<String> resultQueue,
				BlockingQueue<String> getQueue) {
			// save queues for later
			this.commandQueue = commandQueue;
			this.resultQueue = resultQueue;
			this.getQueue = getQueue;
		}

		public void run() {
			// start client
			// client = new Client(commandQueue, resultQueue, getQueue);
			client = new BootStrappedClientStub(commandQueue, resultQueue,
					getQueue);
			// TODO: initialize client on framework
		}
	}

	/**
	 * Thread containing the client and framework
	 */
	private ClientThread clientThread;

	/**
	 * SynchronousQueue containing commands for the client
	 */
	private BlockingQueue<String> commandQueue;

	/**
	 * SynchronousQueue containing results from the client
	 * 
	 * null on success, error as string on failure
	 */
	private BlockingQueue<String> resultQueue;

	/**
	 * SynchronousQueue containing get results
	 */
	private BlockingQueue<String> getQueue;

	public DistributedFileSystem() {
		commandQueue = new SynchronousQueue<String>();
		resultQueue = new SynchronousQueue<String>();
		getQueue = new ArrayBlockingQueue<String>(1);

		clientThread = new ClientThread(commandQueue, resultQueue, getQueue);
		clientThread.start();
	}

	/**
	 * Gives a command to the client and throws an exception on failures
	 */
	private void op(String op) throws DFSException {
		try {
			commandQueue.put(op);
			String result = resultQueue.take();
			if (result.equals("")) {
				return;
			} else {
				throw new DFSException(result);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void append(String filename, String contents) throws DFSException {
		op("append " + filename + " " + contents);
	}

	public void create(String filename) throws DFSException {
		op("create " + filename);
	}

	public void delete(String filename) throws DFSException {
		op("delete " + filename);
	}

	public String get(String filename) throws DFSException {
		op("get " + filename);
		try {
			return getQueue.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return null;
		}
	}

	public void put(String filename, String contents) throws DFSException {
		op("put " + filename + " " + contents);
	}

	public void txAbort() throws DFSException {
		op("txabort");
	}

	public void txCommit() throws DFSException {
		op("txcommit");
	}

	public void txStart() throws DFSException {
		op("txstart");
	}
}
