package edu.washington.cs.cse490h.tdfs;

import edu.washington.cs.cse490h.lib.Utility;

// TODO: HIGH: Define toString, pack in terms of toString

// TODO: Contention friendly ops

abstract class LogEntry {
	static final String entryDelimiter = " ";

	byte[] pack() {
		return Utility.stringToByteArray(toString());
	}

	static LogEntry unpack(byte[] bytes) {
		String msg = Utility.byteArrayToString(bytes);
		Tokenizer t = new Tokenizer(msg, entryDelimiter);
		String cmd = t.next();

		if (cmd.equals("Create")) {
			return new CreateLogEntry();
		} else if (cmd.equals("Delete")) {
			return new DeleteLogEntry();
		} else if (cmd.equals("TXAbort")) {
			String[] filenames = t.rest().split(entryDelimiter);
			return new TXAbortLogEntry(filenames);
		} else if (cmd.equals("TXCommit")) {
			String[] filenames = t.rest().split(entryDelimiter);
			return new TXCommitLogEntry(filenames);
		} else if (cmd.equals("TXStart")) {
			int address = Integer.parseInt(t.next());
			String[] filenames = t.rest().split(entryDelimiter);
			return new TXStartLogEntry(filenames, address);
		} else if (cmd.equals("Write")) {
			boolean append = Boolean.parseBoolean(t.next());
			String content = t.rest();
			return new WriteLogEntry(content, append);
		} else {
			throw new RuntimeException("attempt to unpack invalid operation: "
					+ msg);
		}
	}
}

abstract class MemberLogEntry extends LogEntry {
	int address;

	public MemberLogEntry(int address) {
		this.address = address;
	}
}

abstract class TXLogEntry extends LogEntry {
	String[] filenames;

	public TXLogEntry(String[] filenames) {
		this.filenames = filenames;
	}

	/**
	 * map((+) packedDelimiter, filenames).reduce (+)
	 */
	String joinFilenames() {
		StringBuilder joined = new StringBuilder();
		for (String f : filenames) {
			joined.append(entryDelimiter);
			joined.append(f);
		}
		return joined.toString();
	}
}

class CreateLogEntry extends LogEntry {
	@Override
	public String toString() {
		return "Create";
	}

}

class DeleteLogEntry extends LogEntry {
	@Override
	public String toString() {
		return "Delete";
	}
}

// shouldn't need Get - doesn't mutate

class TXAbortLogEntry extends TXLogEntry {
	public TXAbortLogEntry(String[] filenames) {
		super(filenames);
	}

	@Override
	public String toString() {
		return "TXAbort" + joinFilenames();
	}
}

class TXCommitLogEntry extends TXLogEntry {
	public TXCommitLogEntry(String[] filenames) {
		super(filenames);
	}

	@Override
	public String toString() {
		return "TXCommit" + joinFilenames();
	}
}

class TXStartLogEntry extends TXLogEntry {
	int address;
	
	public TXStartLogEntry(String[] filenames, int address) {
		super(filenames);
		this.address = address;
	}

	@Override
	public String toString() {
		return "TXStart " + address + joinFilenames();
	}
}

class TXTryAbortLogEntry extends TXLogEntry {
	public TXTryAbortLogEntry(String[] filenames) {
		super(filenames);
	}
	
	@Override
	public String toString() {
		return "TXTryAbort" + joinFilenames();
	}
}

class TXTryCommitLogEntry extends TXLogEntry {
	public TXTryCommitLogEntry(String[] filenames) {
		super(filenames);
	}
	
	@Override
	public String toString() {
		return "TXTryCommit" + joinFilenames();
	}
}

class WriteLogEntry extends LogEntry {
	String content;
	boolean append;

	public WriteLogEntry(String content, boolean append) {
		this.content = content;
		this.append = append;
	}

	@Override
	public String toString() {
		return "Write" + entryDelimiter + append + entryDelimiter + content;
	}
}
