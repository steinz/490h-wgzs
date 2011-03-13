package edu.washington.cs.cse490h.tdfs;

public class Transactional<T> {
	private T persistent;
	private T transactional;
	private boolean transacting;

	public Transactional(T defaultValue) {
		this.persistent = defaultValue;
	}

	public void abort() {
		transacting = false;
	}

	public void commit() {
		if (transacting) {
			persistent = transactional;
		}
		transacting = false;
	}

	public T getPersistent() {
		return persistent;
	}

	public T getTransactional() {
		return transacting ? transactional : persistent;
	}

	public void set(T value) {
		if (transacting) {
			transactional = value;
		} else {
			persistent = value;
		}
	}

	public void start() {
		transactional = persistent;
		transacting = true;
	}
}
