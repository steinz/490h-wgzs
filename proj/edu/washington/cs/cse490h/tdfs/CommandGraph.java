package edu.washington.cs.cse490h.tdfs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import edu.washington.cs.cse490h.lib.Callback;

/**
 * A graph of dependent commands and factory of CommandNodes.
 * 
 * TODO: Keep track of orphaned nodes somehow - it would be nice if all of this
 * graph's nodes were either executing in heads, refernced along some path from
 * heads to tails (or refernced as a tail), or on some orphaned list
 */
public class CommandGraph {
	public class CommandNode {
		/**
		 * commands run if this command aborts - can be null
		 */
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
			if (abortCommands != null) {
				for (Command c : abortCommands) {
					try {
						c.execute(node);
					} catch (Exception e) {
						node.printError(e);
					}
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
					checkpointHead = new Tuple<CommandKey, CommandNode>(command
							.getKey(), this);
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

		/**
		 * mutates args
		 */
		public void toDot(Set<String> vertices, Set<String> edges) {
			vertices.add("  " + this.toDotName() + ";");
			for (CommandNode child : this.children) {
				edges
						.add("  " + toDotName() + " -> " + child.toDotName()
								+ ";");
				child.toDot(vertices, edges);
			}
		}

		private String toDotName() {
			return command.getClass().getSimpleName() + "_" + hashCode();
		}

		@Override
		public String toString() {
			return "CommandNode " + hashCode() + " [command=" + command
					+ ", done=" + done + ", checkpoint=" + checkpoint
					+ ", locks=" + locks + ", abortCommands=" + abortCommands
					+ "]";
		}

		/**
		 * Appends toString to indent and recursively calls on children with
		 * increased indent.
		 * 
		 * If the node has already been printed during this traversal, just
		 * prints the node's hashCode as a reference.
		 * 
		 * Note: pass sb through?
		 */
		public String toString(String indent, Set<CommandNode> alreadyPrinted) {
			StringBuilder sb = new StringBuilder();
			if (alreadyPrinted.contains(this)) {
				sb.append(indent + "CommandNode " + hashCode() + "\n");
			} else {
				sb.append(indent + this.toString() + "\n");
				alreadyPrinted.add(this);
			}
			for (CommandNode child : this.children) {
				sb.append(child.toString(indent + "  ", alreadyPrinted));
			}
			return sb.toString();
		}
	}

	private static int commandRetryTimeout = 20;

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

	/**
	 * Creates a new CommandNode and adds it to the graph
	 */
	protected CommandNode addCommand(Command c, boolean checkpoint,
			List<Command> abortCommands) {
		CommandNode n = newCommand(c, checkpoint, abortCommands);
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

	public void addDependency(CommandNode parent, CommandNode child) {
		if (!parent.done) {
			parent.children.add(child);
			child.locks++;
		}
	}

	public boolean done(CommandKey key) {
		if (checkpointHead != null
				&& checkpointHead.second.command.getKey().reallyEquals(key)) {
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
				if (c.getKey().reallyEquals(key)) {
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

	/**
	 * Creates a new CommandNode without adding it to the graph
	 */
	public CommandNode newCommand(Command c, boolean checkpoint,
			List<Command> abortCommands) {
		return new CommandNode(c, checkpoint, abortCommands);
	}

	private void start() {
		checkpointHead = null;
		checkpointTail = null;
		heads.clear();
		tails.clear();
	}

	public String toDot() {
		Set<String> edges = new HashSet<String>();
		Set<String> vertices = new HashSet<String>();
		if (checkpointHead != null) {
			checkpointHead.second.toDot(vertices, edges);
		}
		for (Entry<CommandKey, CommandNode> e : heads.entrySet()) {
			e.getValue().toDot(vertices, edges);
		}

		StringBuilder sb = new StringBuilder();
		sb.append("digraph commands {\n");
		for (String vertex : vertices) {
			sb.append(vertex + "\n");
		}
		for (String edge : edges) {
			sb.append(edge + "\n");
		}
		sb.append("}\n");
		return sb.toString();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Set<CommandNode> alreadyPrinted = new HashSet<CommandNode>();
		if (checkpointHead != null) {
			sb.append(checkpointHead.second.toString("", alreadyPrinted));
		} else {
			for (Entry<CommandKey, CommandNode> e : heads.entrySet()) {
				sb.append(e.getValue().toString("", alreadyPrinted));
			}
		}
		if (checkpointTail != null) {
			sb.append("checkpointTail: " + checkpointTail.toString() + "\n");
		}
		if (tails.size() > 0) {
			sb.append("\ntails: ");
			for (Entry<CommandKey, CommandNode> e : tails.entrySet()) {
				sb.append(e.getValue().toString() + ", ");
			}
		}
		return sb.toString();
	}
}
