package edu.washington.cs.cse490h.tdfs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A DAG of dependent commands to execute. A factory of CommandNodes.
 */
public class CommandDAG<K> {
	/**
	 * Encapsulates a function to be run by a CommandNode
	 */
	public static abstract class Command<AK, AV> {
		public abstract void execute(Map<AK, AV> args);
	}

	/**
	 * Encapsulates a single command the user wants to execute.
	 */
	public abstract class CommandNode<AK, AV, RK, RV> {

		/**
		 * A list of CommandNodes dependent on the successful execution of this
		 * CommandNode.
		 */
		private List<CommandNode<RK, RV, ?, ?>> children;
		/**
		 * The function that this CommandNode executes.
		 */
		private Command<AK, AV> command;
		/**
		 * The function that this CommandNode executes on failure.
		 */
		private Command<AK, AV> failCommand;
		/**
		 * A key for this CommandNode unique amongst executing Commands
		 */
		private K key;
		/**
		 * The number of parents this CommandNode has.
		 */
		private int locks;
		/**
		 * Args for this command.
		 */
		private Map<AK, AV> args;

		/**
		 * Creates a dangling CommandNode in the graph with the given commands.
		 */
		public CommandNode(Command<AK, AV> command,
				Command<AK, AV> failCommand, K key) {
			this.command = command;
			this.failCommand = failCommand;
			this.key = key;
			this.locks = 0;

			dangling.put(key, this);
		}

		private void addChild(CommandNode<RK, RV, ?, ?> child) {
			children.add(child);
			child.locks++;
		}

		/**
		 * Notify children of results.
		 */
		private void done(Map<RK, RV> results) {
			executing.remove(this);
			for (CommandNode<RK, RV, ?, ?> child : children) {
				child.parentFinished(results);
			}
		}

		/**
		 * Execute this Command.
		 */
		private void execute() {
			CommandNode<?, ?, AK, AV> p = (CommandNode<?, ?, AK, AV>) executing.get(key);
			if (p == null) {
				executing.put(key, this);
				command.execute(this.args);
			} else {
				p.addChild(this);
			}
		}

		/**
		 * Fail this command.
		 */
		private void fail() {
			executing.remove(key);
			failCommand.execute(this.args);
		}

		/**
		 * Adds parent's results to args and executes if unlocked.
		 */
		private void parentFinished(Map<AK, AV> args) {
			this.args.putAll(args);
			this.locks--;
			if (this.locks == 0) {
				execute();
			}
		}
	}

	private Map<K, CommandNode<?, ?, ?, ?>> dangling;
	private Map<K, CommandNode<?, ?, ?, ?>> executing;
	private Map<K, CommandNode<?, ?, ?, ?>> execTails;

	public CommandDAG() {
		this.dangling = new HashMap<K, CommandNode<?, ?, ?, ?>>();
		this.executing = new HashMap<K, CommandNode<?, ?, ?, ?>>();
		this.execTails = new HashMap<K, CommandNode<?, ?, ?, ?>>();
	}

	public void done(K key) {
		executing.get(key).done(null);
	}

	public void exec(K key) {
		dangling.get(key).execute();
	}

	public void fail(K key) {
		executing.get(key).fail();
	}
}
