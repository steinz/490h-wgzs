package edu.washington.cs.cse490h.tdfs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

/**
 * A graph of dependent commands
 */
public class CommandGraph {
	public class CommandNode {
		private Command command;
		private int locks;
		private List<CommandNode> children;

		public CommandNode(Command c) {
			this.command = c;
			this.locks = 0;
			this.children = new ArrayList<CommandNode>();
		}

		public void execute() {
			/*
			 * TODO: HIGH: logFS should probably be private, fix how
			 * command.execute accesses things maybe
			 */
			executingCommands.add(this);
			command.execute(node, node.logFS);
		}

		public void done() {
			executingCommands.remove(this);
			for (CommandNode node : children) {
				node.parentFinished();
			}
		}

		public void parentFinished() {
			locks--;
			if (locks == 0) {
				execute();
			}
		}
	}

	private TDFSNode node;
	private Set<CommandNode> executingCommands;
	private Map<String, CommandNode> tails;
	private CommandNode checkpoint;

	public CommandGraph(TDFSNode node) {
		this.node = node;
	}

	public CommandNode addCommand(FileCommand c) {
		CommandNode n = new CommandNode(c);
		CommandNode p = tails.get(c.filename);
		if (p == null) {
			p = checkpoint;
		}
		if (p != null) {
			addEdge(p, n);
		} else {
			n.execute();
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
		checkpoint = n;

		return n;
	}

	public void addEdge(CommandNode parent, CommandNode child) {
		parent.children.add(child);
		child.locks++;
	}
}
