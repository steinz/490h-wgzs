package edu.washington.cs.cse490h.tdfs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.washington.cs.cse490h.lib.Callback;

/**
 * A graph of dependent commands and factory of CommandNodes
 */
public class CommandGraph {
	private static int commandRetryTimeout = 20;

	public class CommandNode {
		private Command command;
		private int locks;
		private List<CommandNode> children;
		private boolean done;

		public CommandNode(Command c) {
			this.command = c;
			this.locks = 0;
			this.children = new ArrayList<CommandNode>();
			this.done = false;
		}

		/**
		 * Returns true if the command is already done, otherwise returns
		 * whether or not execute actually executed
		 */
		public boolean execute() {
			if (done) {
				return true;
			}

			if (this.locks == 0) {
				// TODO: OPT: Fail after x retrys, declare file unavailable
				try {
					String[] params = {};
					Object[] args = {};
					Callback cb = new Callback(Callback.getMethod("execute",
							this, params), this, args);
					node.addTimeout(cb, commandRetryTimeout);
				} catch (Exception e) {
					node.printError(e);
				}

				if (command instanceof FileCommand) {
					heads.put(command.filename, this);
				} else {
					checkpointHead = this;
					checkpointHeadFilename = command.filename;
				}
				try {
					command.execute(node);
				} catch (Exception e) {
					node.printError(e);
					abort();
				}
			}
			return this.locks == 0;
		}

		public void abort() {
			checkpointHead = checkpointTail = null;
			checkpointHeadFilename = null;
			heads.clear();
			tails.clear();
			abortRecur();
		}

		private void abortRecur() {
			this.done = true;
			for (CommandNode node : children) {
				node.abortRecur();
			}
		}

		public void done() {
			this.done = true;
			for (CommandNode node : children) {
				node.parentFinished();
			}
		}

		public void parentFinished() {
			locks--;
			execute();
		}
	}

	private TDFSNode node;
	private Map<String, CommandNode> heads;
	private Map<String, CommandNode> tails;

	private String checkpointHeadFilename;
	private CommandNode checkpointHead;
	private CommandNode checkpointTail;

	public CommandGraph(TDFSNode node) {
		this.node = node;
		this.heads = new HashMap<String, CommandNode>();
		this.tails = new HashMap<String, CommandNode>();
	}

	public CommandNode addCommand(FileCommand c) {
		CommandNode n = new CommandNode(c);
		CommandNode p = tails.get(c.filename);
		if (p == null) {
			p = checkpointTail;
		}
		if (p != null) {
			addEdge(p, n);
		}
		tails.put(c.filename, n);
		return n;
	}

	public CommandNode addCheckpoint(Command c) {
		CommandNode n = new CommandNode(c);

		for (Entry<String, CommandNode> entry : tails.entrySet()) {
			addEdge(entry.getValue(), n);
		}
		if (checkpointTail != null) {
			addEdge(checkpointTail, n);
		}

		tails.clear();
		checkpointTail = n;

		return n;
	}

	// TODO: dedup multiple edges from parent to child

	public void addEdge(CommandNode parent, CommandNode child) {
		parent.children.add(child);
		child.locks++;
	}

	public void checkpointDone(String filename) {
		if (checkpointHeadFilename == filename) {
			try {
				if (checkpointTail == checkpointHead) {
					checkpointTail = null;
				}
				checkpointHead.done();
				checkpointHead = null;
				checkpointHeadFilename = null;
			} catch (NullPointerException e) {
				throw new RuntimeException(
						"checkpointDone called when no checkpoint set", e);
			}
		}
	}

	public void filenameDone(String filename, int operationNumber,
			int proposalNumber) {
		CommandNode n = heads.get(filename);
		if (n == null) {
			return;
		}
		Command c = n.command;
		if (c.operationNumber == operationNumber
				&& c.proposalNumber == proposalNumber) {
			try {
				CommandNode head = 	heads.remove(filename);
				CommandNode tail = tails.get(filename);
				if (tail.equals(head)) {
					tails.remove(filename);
				}
				head.done();
			} catch (NullPointerException e) {
				throw new RuntimeException(
						"filenameDone called on unlocked filename", e);
			}
		}
	}
}
