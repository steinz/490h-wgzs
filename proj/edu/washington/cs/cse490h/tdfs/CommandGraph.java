package edu.washington.cs.cse490h.tdfs;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import edu.washington.cs.cse490h.lib.Callback;

/**
 * A graph of dependent commands (and factory of CommandNodes?)
 * 
 * TODO: HIGH: Formalize: Queues commands on Keys with the same hashCode - done
 * called on commands that are reallyEquals
 * 
 * TODO: Keep track of orphaned nodes somehow - it would be nice if all of this
 * graph's nodes were either executing in heads, referenced along some path from
 * heads to tails (or referenced as a tail), or on some orphaned list
 */
public class CommandGraph {
	/**
	 * Wraps a command waiting to be executed, keeping track of commands that
	 * depend on it (children), what to do if it aborts, etc.
	 */
	public class CommandNode {
		/**
		 * Commands run if this command aborts - can be null.
		 * 
		 * Commands run on aborts are not retried if they fail.
		 */
		private List<Command> abortCommands;
		/**
		 * The command to run once all parents are done.
		 */
		private Command command;
		/**
		 * Number of parents not yet done.
		 */
		private int locks;
		/**
		 * List of nodes depending on this command's completion.
		 */
		private Set<CommandNode> children;
		/**
		 * Whether this node is a checkpoint - all commands added after a
		 * checkpoint depend on that checkpoint.
		 */
		private boolean checkpoint;
		/**
		 * Whether or not this node has successfully executed, been canceled, or
		 * been aborted.
		 */
		private boolean done;

		/**
		 * Creates a new CommandNode. CommandNodes created this way should be
		 * explicitly added to a CommandGraph using
		 * CommandGraph.addCommand(CommandNode).
		 * 
		 * Creates nodes with CommandGraph.addCommand(Command, boolean,
		 * List<Command>) if you don't care when they get added to the graph
		 * (this will add them immediately).
		 * 
		 * Takes the command to be executed, whether or not this node is a
		 * checkpoint, and a list of commands to attempt to execute once if the
		 * command aborts.
		 */
		private CommandNode(Command c, boolean checkpoint,
				List<Command> abortCommands) {
			this.abortCommands = abortCommands;
			this.command = c;
			this.locks = 0;
			this.children = new HashSet<CommandNode>();
			this.checkpoint = checkpoint;
			this.done = false;
		}

		/**
		 * Tries to execute the command in this node's abortCommands list once
		 * and then cancels this node and all of it's children.
		 */
		public void abort() {
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

		/**
		 * Add a dependency from this node to child if this node isn't done.
		 */
		private void addChild(CommandNode child) {
			if (!done && !children.contains(child)) {
				child.locks++;
				children.add(child);
			}
		}

		/**
		 * Add a dependency from parent to this node if parent isn't done.
		 */
		private void addParent(CommandNode parent) {
			parent.addChild(this);
		}

		/**
		 * Sets this node's done flag and recursively calls cancel on all of
		 * it's children. Call abort if you want this client's abortCommands
		 * list to be executed first.
		 */
		public void cancel() {
			this.done = true;
			for (CommandNode node : children) {
				node.cancel();
			}
		}

		/**
		 * Sets this node's done flag, decrements the lock count on each of it's
		 * children and executes them if their lock count is 0 (they have no
		 * dependencies left).
		 * 
		 * This should only be called once.
		 */
		public void done() {
			if (!this.done) {
				this.done = true;
				for (CommandNode node : children) {
					node.locks--;
					if (node.locks == 0) {
						node.execute();
					}
				}
			}
		}

		/**
		 * Should be called externally after adding the CommandNode to the graph
		 * with CommandGraph.addCommand (or constructing dependencies
		 * externally, although this is not recommended).
		 * 
		 * Returns true if the command has already been marked done or is
		 * successfully executed (at which point it should be marked as done by
		 * an external call to done or cancel).
		 * 
		 * This command will retry every commandRetryTimeout rounds until it is
		 * marked done.
		 * 
		 * Calls abort if command.execute throws an exception.
		 */
		public boolean execute() {
			if (this.done) {
				return true;
			} else if (this.locks == 0) {
				// register retry
				try {
					String[] params = {};
					Object[] args = {};
					Callback cb = new Callback(Callback.getMethod("execute",
							this, params), this, args);
					node.addTimeout(cb, commandRetryTimeout);
				} catch (Exception e) {
					node.printError(e);
				}

				// update CommandGraph maps of executing nodes
				if (checkpoint) {
					checkpointHead = this;
				} else {
					heads.put(command.getKey(), this);
				}

				// try and execute the command, abort if it throws an exception
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
			String thisFile = this.command.filename.replace('.', '_');
			String thisName = "";
			if (checkpoint) {
				thisName += "_";
			}
			thisName += this.command.getName() + "_" + thisFile;
			thisName += "_" + hashCode();

			vertices.add("  " + thisName + ";");
			for (CommandNode child : this.children) {
				String thatFile = child.command.filename.replace('.', '_');
				String thatName = "";
				if (child.checkpoint) {
					thatName += "_";
				}
				thatName += child.command.getName() + "_" + thatFile;
				thatName += "_" + child.hashCode();

				edges.add("  " + thisName + " -> " + thatName + ";");
				child.toDot(vertices, edges);
			}
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

	/**
	 * Number of rounds to wait before retrying executed commands.
	 */
	private static int commandRetryTimeout = 20;

	/**
	 * A reference to the underlying node used for logging and callback
	 * registration.
	 */
	private TDFSNode node;
	/**
	 * The currently executing checkpoint command or null.
	 */
	private CommandNode checkpointHead;
	/**
	 * The last checkpoint command added but not yet executed.
	 */
	private CommandNode checkpointTail;
	/**
	 * Currently executing commands.
	 */
	private Map<CommandKey, CommandNode> heads;
	/**
	 * Commands added but not yet executed.
	 */
	private Map<CommandKey, CommandNode> tails;

	/**
	 * Constructs a new commandGraph referencing node.
	 */
	public CommandGraph(TDFSNode node) {
		this.checkpointHead = null;
		this.checkpointTail = null;
		this.node = node;
		this.heads = new HashMap<CommandKey, CommandNode>();
		this.tails = new HashMap<CommandKey, CommandNode>();
	}

	/**
	 * Adds CommandNode n to the graph and returns it.
	 * 
	 * Checkpoint commands depend on everything added before them and are
	 * parents to everything added after them. Non-checkpoint commands
	 */
	public CommandNode addCommand(CommandNode n) {
		if (n.checkpoint) {
			for (Entry<CommandKey, CommandNode> entry : tails.entrySet()) {
				addDependency(entry.getValue(), n);
			}
			if (checkpointTail != null && tails.size() == 0) {
				addDependency(checkpointTail, n);
			}
			tails.clear();
			checkpointTail = n;
		} else {
			CommandKey key = n.command.getKey();
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

	/**
	 * Constructs a new CommandNode, adds it to the graph, and returns it.
	 */
	public CommandNode addCommand(Command command, boolean checkpoint,
			List<Command> abortCommands) {
		return addCommand(new CommandNode(command, checkpoint, abortCommands));
	}

	/**
	 * Add child to parent's list of dependent children.
	 */
	public void addDependency(CommandNode parent, CommandNode child) {
		parent.addChild(child);
	}

	/**
	 * Looks for executing commands reallyEquals to this key and calls their
	 * done method.
	 * 
	 * Returns whether or not such a command was found.
	 */
	public boolean done(CommandKey key) {
		if (checkpointHead != null
				&& checkpointHead.command.getKey().reallyEquals(key)) {
			if (checkpointTail == checkpointHead) {
				checkpointTail = null;
			}
			checkpointHead.done();
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

	public CommandNode noop() {
		return new CommandNode(new Command("", -1) {
			@Override
			public void execute(TDFSNode node) throws Exception {
				node.commandGraph.done(new CommandKey("", -1));
			}

			@Override
			public CommandKey getKey() {
				return new CommandKey("", -1);
			}
		}, false, null);
	}

	public String toDot() {
		Set<String> edges = new HashSet<String>();
		Set<String> vertices = new HashSet<String>();
		if (checkpointHead != null) {
			checkpointHead.toDot(vertices, edges);
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
			sb.append(checkpointHead.toString("", alreadyPrinted));
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
