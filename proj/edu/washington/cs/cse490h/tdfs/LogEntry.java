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
		int start = 0;
		int stop = msg.indexOf(packetDelimiter);
		String cmd;
		if (stop == -1) {
			cmd = msg;
		} else {
			cmd = msg.substring(start, stop);
		}
		if (cmd.equals("Create")) {
			return new CreateLogEntry();
		} else if (cmd.equals("Delete")) {
			return new DeleteLogEntry();
		} else if (cmd.equals("Lock")) {
			start = stop + packetDelimiter.length();
			int address = Integer.parseInt(msg.substring(start));
			return new LockLogEntry(address);
		} else if (cmd.equals("TXAbort")) {
			return new TXAbortLogEntry();
		} else if (cmd.equals("TXCommit")) {
			return new TXCommitLogEntry();
		} else if (cmd.equals("TXStart")) {
			return new TXStartLogEntry();
		} else if (cmd.equals("Unlock")) {
			start = stop + packetDelimiter.length();
			int address = Integer.parseInt(msg.substring(start));
			return new UnlockLogEntry(address);
		} else if (cmd.equals("Write")) {
			start = stop + packetDelimiter.length();
			stop = msg.indexOf(packetDelimiter, start);
			boolean append = Boolean.parseBoolean(msg.substring(start, stop));
			start = stop + packetDelimiter.length();
			String content = msg.substring(start);
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

class TXAbortLogEntry extends LogEntry {
	@Override
	public String toString() {
		return "TXAbort";
	}
}

class TXCommitLogEntry extends LogEntry {
	@Override
	public String toString() {
		return "TXCommit";
	}
}

class TXStartLogEntry extends LogEntry {
	@Override
	public String toString() {
		return "TXStart";
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
