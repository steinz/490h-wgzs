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
 * A graph of dependent commands
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
			try {
				String[] params = { "edu.washington.cs.cse490h.tdfs.TDFSNode",
						"edu.washington.cs.cse490h.tdfs.LogFS" };
				Object[] args = { node, node.logFS };
				Callback cb = new Callback(Callback.getMethod("retry", command,
						params), command, args);
				node.addTimeout(cb, commandRetryTimeout);
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			// TODO: HIGH: handle exceptions

			/*
			 * TODO: HIGH: logFS should probably be private, fix how
			 * command.execute accesses things maybe
			 */
			if (this.locks == 0) {
				executingCommands.add(this);
				command.execute(node, node.logFS);
			}
			return this.locks == 0;
		}

		public void done() {
			executingCommands.remove(this);
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
	private Set<CommandNode> executingCommands;
	private Map<String, CommandNode> tails;
	private CommandNode checkpoint;

	public CommandGraph(TDFSNode node) {
		this.node = node;
		this.executingCommands = new HashSet<CommandNode>();
		this.tails = new HashMap<String, CommandNode>();
	}

	public CommandNode addCommand(FileCommand c) {
		CommandNode n = new CommandNode(c);
		CommandNode p = tails.get(c.filename);
		if (p == null) {
			p = checkpoint;
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
		checkpoint = n;

		return n;
	}

	// TODO: HIGH: multiple edges from parent to child allowed?

	public void addEdge(CommandNode parent, CommandNode child) {
		parent.children.add(child);
		child.locks++;
	}
}
