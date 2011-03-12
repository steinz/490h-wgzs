package edu.washington.cs.cse490h.tdfs;

abstract class SubKey {
}

class IntegerSubKey extends SubKey {
	public final Integer value;

	public IntegerSubKey(int v) {
		this.value = v;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IntegerSubKey other = (IntegerSubKey) obj;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}
}

class IntegerTupleSubKey extends SubKey {
	public final Tuple<Integer, Integer> value;

	public IntegerTupleSubKey(int v1, int v2) {
		this.value = new Tuple<Integer, Integer>(v1, v2);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IntegerTupleSubKey other = (IntegerTupleSubKey) obj;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}
}

public class CommandKey extends Tuple<String, SubKey> {

	public CommandKey(String filename, int address) {
		super(filename, new IntegerSubKey(address));
	}

	public CommandKey(String filename, int operationNumber, int proposalNumber) {
		super(filename, new IntegerTupleSubKey(operationNumber, proposalNumber));
	}

	/**
	 * hash only depends on filename since proposal number and operation number
	 * are mutated when the command executes
	 */
	@Override
	public int hashCode() {
		return this.first.hashCode();
	}

	/**
	 * equals only depends on filename for the command graph's hash tables to
	 * chain commands correctly
	 * 
	 * CommandKeys are equal iff refer to the same filename
	 * 
	 * CommandKeys are reallyEqual iff they also refer to the same SubKey
	 * 
	 * use reallyEquals to check if the subkeys match too
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof CommandKey) {
			CommandKey other = (CommandKey) obj;
			return this.first.equals(other.first);
		} else {
			return false;
		}
	}

	/**
	 * checks that the (filename, subkey) tuples are equal
	 * 
	 * CommandKeys are equal iff refer to the same filename
	 * 
	 * CommandKeys are reallyEqual iff they also refer to the same SubKey
	 */
	public boolean reallyEquals(CommandKey other) {
		return super.equals(other);
	}
}
