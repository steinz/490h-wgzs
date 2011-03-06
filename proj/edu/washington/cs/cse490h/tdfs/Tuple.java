package edu.washington.cs.cse490h.tdfs;

class Tup {
	private final Class<?>[] types;
	private Object[] values;
	
	public Tup(int size, Class<?>[] types, Object[] values) {
		this.types = types;
		for (int i = 0; i < values.length; i++) {
			set(i, values[i]);
		}
	}
	
	public Class<?> getType(int index) {
		return types[index];
	}
	
	public Object get(int index) {
		return values[index];
	}
	
	public void set(int index, Object value) {
		values[index] = types[index].cast(value);
	}
}

public class Tuple<T, U> {
	public final T first;
	public final U second;

	public Tuple(T first, U second) {
		this.first = first;
		this.second = second;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
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
		Tuple<?,?> other = (Tuple<?,?>) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second == null) {
			if (other.second != null)
				return false;
		} else if (!second.equals(other.second))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "(" + first + "," + second + ")";
	}
}
