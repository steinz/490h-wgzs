package edu.washington.cs.cse490h.tdfs;

public class Tokenizer {
	private String str;
	private String delim;

	public Tokenizer(String str, String delim) {
		this.str = str;
		this.delim = delim;
	}

	public String next() {
		int d = str.indexOf(delim);
		String result;
		if (d == -1) {
			result = str;
			str = null;
		} else {
			result = str.substring(0, d);
			str = str.substring(d + delim.length());
		}
		return result;
	}

	public String rest() {
		String result = str;
		str = null;
		return result;
	}
}