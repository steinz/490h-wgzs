package edu.washington.cs.cse490h.tdfs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.washington.cs.cse490h.lib.Callback;

public class CommandGraph {
	private static int commandRetryTimeout = 20;

	private class CommandNode {
		private List<Command> abortCommands;
		private Command command;
		private int locks;
		private List<CommandNode> children;
		private boolean checkpoint;
		private boolean done;

		public CommandNode(Command c, boolean checkpoint,
				List<Command> abortCommands) {
			this.abortCommands = abortCommands;
			this.command = c;
			this.locks = 0;
			this.children = new ArrayList<CommandNode>();
			this.checkpoint = checkpoint;
			this.done = false;
		}

		public void abort() {
			start();
			for (Command c : abortCommands) {
				try {
					c.execute(node);
				} catch (Exception e) {
					node.printError(e);
				}
			}
			cancel();
		}

		private void cancel() {
			this.done = true;
			for (CommandNode node : children) {
				node.cancel();
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

		/**
		 * returns true if the command has already been marked done or is
		 * executed
		 */
		public boolean execute() {
			if (this.done) {
				return true;
			} else if (this.locks == 0) {
				try {
					String[] params = {};
					Object[] args = {};
					Callback cb = new Callback(Callback.getMethod("execute",
							this, params), this, args);
					node.addTimeout(cb, commandRetryTimeout);
				} catch (Exception e) {
					node.printError(e);
				}

				if (checkpoint) {
					checkpointHead = new Tuple<CommandKey, CommandNode>(command.getKey(), this);
				} else {
					heads.put(command.getKey(), this);
				}

				try {
					command.execute(node);
				} catch (Exception e) {
					node.printError(e);
					abort();
				}
				return true;
			} else {
				return false;
			}
		}

	}

	private TDFSNode node;
	private Tuple<CommandKey, CommandNode> checkpointHead;
	private CommandNode checkpointTail;
	private Map<CommandKey, CommandNode> heads;
	private Map<CommandKey, CommandNode> tails;

	public CommandGraph(TDFSNode node) {
		this.node = node;
		this.heads = new HashMap<CommandKey, CommandNode>();
		this.tails = new HashMap<CommandKey, CommandNode>();
		start();
	}

	public CommandNode addCommand(Command c, boolean checkpoint,
			List<Command> abortCommands) {
		CommandNode n = new CommandNode(c, checkpoint, abortCommands);
		if (checkpoint) {
			for (Entry<CommandKey, CommandNode> entry : tails.entrySet()) {
				addDependency(entry.getValue(), n);
			}
			if (checkpointTail != null) {
				addDependency(checkpointTail, n);
			}
			tails.clear();
			checkpointTail = n;
		} else {
			CommandKey key = c.getKey();
			CommandNode p = tails.get(key);
			if (p == null) {
				p = checkpointTail;
			}
			if (p != null) {
				addDependency(p, n);
			}
			tails.put(key, n);
		}
		return n;
	}

	public boolean done(CommandKey key) {
		if (checkpointHead != null && checkpointHead.second.equals(key)) {
			if (checkpointTail == checkpointHead.second) {
				checkpointTail = null;
			}
			checkpointHead.second.done();
			checkpointHead = null;
			return true;
		} else {
			CommandNode n = heads.get(key);
			if (n == null) {
				return false;
			} else {
				Command c = n.command;
				if (c.getKey().equals(key)) {
					CommandNode head = heads.remove(key);
					CommandNode tail = tails.get(key);
					if (tail == head) {
						tails.remove(key);
					}
					head.done();
					return true;
				} else {
					return false;
				}
			}
		}
	}

	public void addDependency(CommandNode parent, CommandNode child) {
		if (!parent.done) {
			parent.children.add(child);
			child.locks++;
		}
	}

	private void start() {
		checkpointHead = null;
		checkpointTail = null;
		heads.clear();
		tails.clear();
	}
}
