package edu.washington.cs.cse490h.tdfs;

import java.util.HashMap;
import java.util.Map;

public class LRUCache<K, V> {
	private class Node {
		Node prev;
		Node next;
		V value;

		public Node(V value) {
			this.value = value;
		}

		public void remove() {
			prev.next = next;
			next.prev = prev;
		}

		public void setNext(Node n) {
			n.next = next;
			n.prev = this;
			next = n;
			next.prev = n;
		}
	}

	public int capacity;
	public int size;
	private Node first, last;
	private Map<K, Node> hash;

	public LRUCache(int capacity) {
		this.capacity = capacity;
		this.size = 0;
		this.first = new Node(null);
		this.last = new Node(null);
		first.next = last;
		last.prev = first;
		this.hash = new HashMap<K, Node>();
	}

	public void put(K k, V v) {
		Node n = new Node(v);
		hash.put(k, n);
		size += 1;
		if (size > capacity) {
			last.prev.remove();
		}
		first.setNext(n);
	}

	public V get(K k) {
		Node n = hash.get(k);
		if (n == null) {
			return null;
		} else {
			n.remove();
			first.setNext(n);
			return n.value;
		}
	}
}