package edu.washington.cs.cse490h.tdfs;

import edu.washington.cs.cse490h.lib.Utility;

// TODO: HIGH: Define toString, pack in terms of toString

// TODO: Contention friendly ops

abstract class LogEntry {
	static String packetDelimiter = " ";

	byte[] pack() {
		return Utility.stringToByteArray(toString());
	}

	static LogEntry unpack(byte[] bytes) {
		String msg = Utility.byteArrayToString(bytes);
		Tokenizer t = new Tokenizer(msg, packetDelimiter);
		String cmd = t.next();

		if (cmd.equals("Create")) {
			return new CreateLogEntry();
		} else if (cmd.equals("Delete")) {
			return new DeleteLogEntry();
		} else if (cmd.equals("Lock")) {
			int address = Integer.parseInt(t.next());
			return new LockLogEntry(address);
		} else if (cmd.equals("TXAbort")) {
			String[] filenames = t.rest().split(packetDelimiter);
			return new TXAbortLogEntry(filenames);
		} else if (cmd.equals("TXCommit")) {
			String[] filenames = t.rest().split(packetDelimiter);
			return new TXCommitLogEntry(filenames);
		} else if (cmd.equals("TXStart")) {
			String[] filenames = t.rest().split(packetDelimiter);
			return new TXStartLogEntry(filenames);
		} else if (cmd.equals("Unlock")) {
			int address = Integer.parseInt(t.next());
			return new UnlockLogEntry(address);
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
			joined.append(packetDelimiter);
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

class LockLogEntry extends MemberLogEntry {
	public LockLogEntry(int address) {
		super(address);
	}

	@Override
	public String toString() {
		return "Lock" + packetDelimiter + address;
	}
}

// TODO: HIGH: ZACH: Add filenames to TXLogEntries

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
	public TXStartLogEntry(String[] filenames) {
		super(filenames);
	}

	@Override
	public String toString() {
		return "TXStart" + joinFilenames();
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

class UnlockLogEntry extends MemberLogEntry {
	public UnlockLogEntry(int address) {
		super(address);
	}

	@Override
	public String toString() {
		return "Unlock" + packetDelimiter + address;
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
		return "Write" + packetDelimiter + append + packetDelimiter + content;
	}
}
