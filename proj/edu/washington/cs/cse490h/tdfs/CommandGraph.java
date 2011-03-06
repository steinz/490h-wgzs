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

		public CommandNode(Command c) {
			this.command = c;
			this.locks = 0;
			this.children = new ArrayList<CommandNode>();
		}

		public boolean execute() {
			// TODO: OPT: Fail after x retrys, declare file unavailable
			try {
				String[] params = { "edu.washington.cs.cse490h.tdfs.TDFSNode" };
				Object[] args = { node };
				Callback cb = new Callback(Callback.getMethod("retry", command,
						params), command, args);
				node.addTimeout(cb, commandRetryTimeout);
			} catch (Exception e) {
				node.printError(e);
			}

			if (this.locks == 0) {
				if (command instanceof FileCommand) {
					FileCommand fc = (FileCommand) command;
					heads.put(fc.filename, this);
				} else {
					checkpointHead = this;
				}
				command.execute(node);
			}
			return this.locks == 0;
		}

		public void done() {
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
		tails.clear();
		checkpointTail = n;

		return n;
	}

	// TODO: dedup multiple edges from parent to child

	public void addEdge(CommandNode parent, CommandNode child) {
		parent.children.add(child);
		child.locks++;
	}

	public void checkpointDone() {
		try {
			checkpointHead.done();
			checkpointHead = null;
		} catch (NullPointerException e) {
			throw new RuntimeException(
					"checkpointDone called when no checkpoint set", e);
		}
	}

	public void filenameDone(String filename) {
		try {
			heads.remove(filename).done();
		} catch (NullPointerException e) {
			throw new RuntimeException(
					"filenameDone called on unlocked filename", e);
		}
	}
}
