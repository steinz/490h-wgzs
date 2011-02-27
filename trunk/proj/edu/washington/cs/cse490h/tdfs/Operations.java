package edu.washington.cs.cse490h.tdfs;

import edu.washington.cs.cse490h.lib.Utility;

// TODO: HIGH: Define toString, pack in terms of toString

// TODO: Contention friendly ops

abstract class Operation {
	static String packetDelimiter = " ";

	byte[] pack() {
		return Utility.stringToByteArray(toString());
	}

	static Operation unpack(byte[] bytes) {
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
			return new Create();
		} else if (cmd.equals("Delete")) {
			return new Delete();
		} else if (cmd.equals("Forgotten")) {
			return new Forgotten();
		} else if (cmd.equals("Join")) {
			start = stop + packetDelimiter.length();
			int address = Integer.parseInt(msg.substring(start));
			return new Join(address);
		} else if (cmd.equals("Leave")) {
			start = stop + packetDelimiter.length();
			int address = Integer.parseInt(msg.substring(start));
			return new Leave(address);
		} else if (cmd.equals("Lock")) {
			start = stop + packetDelimiter.length();
			int address = Integer.parseInt(msg.substring(start));
			return new Lock(address);
		} else if (cmd.equals("TXAbort")) {
			return new TXAbort();
		} else if (cmd.equals("TXCommit")) {
			return new TXCommit();
		} else if (cmd.equals("TXStart")) {
			return new TXStart();
		} else if (cmd.equals("Unlock")) {
			start = stop + packetDelimiter.length();
			int address = Integer.parseInt(msg.substring(start));
			return new Unlock(address);
		} else if (cmd.equals("Write")) {
			start = stop + packetDelimiter.length();
			stop = msg.indexOf(packetDelimiter, start);
			boolean append = Boolean.parseBoolean(msg.substring(start, stop));
			start = stop + packetDelimiter.length();
			String content = msg.substring(start);
			return new Write(content, append);
		} else {
			throw new RuntimeException("attempt to unpack invalid operation: "
					+ msg);
		}

	}
}

abstract class FileOperation extends Operation {
}

abstract class MemberOperation extends Operation {
	int address;

	public MemberOperation(int address) {
		this.address = address;
	}
}

class Create extends FileOperation {
	@Override
	public String toString() {
		return "Create";
	}

}

class Delete extends FileOperation {
	@Override
	public String toString() {
		return "Delete";
	}
}

class Forgotten extends Operation {
	@Override
	public String toString() {
		return "Forgotten";
	}
}

// shouldn't need Get - doesn't mutate

class Join extends MemberOperation {
	public Join(int address) {
		super(address);
	}

	@Override
	public String toString() {
		return "Join" + packetDelimiter + address;
	}
}

class Leave extends MemberOperation {
	public Leave(int address) {
		super(address);
	}

	@Override
	public String toString() {
		return "Leave" + packetDelimiter + address;
	}
}

class Lock extends MemberOperation {
	public Lock(int address) {
		super(address);
	}

	@Override
	public String toString() {
		return "Lock" + packetDelimiter + address;
	}
}

class TXAbort extends Operation {
	@Override
	public String toString() {
		return "TXAbort";
	}
}

class TXCommit extends Operation {
	@Override
	public String toString() {
		return "TXCommit";
	}
}

class TXStart extends Operation {
	@Override
	public String toString() {
		return "TXStart";
	}
}

class Unlock extends MemberOperation {
	public Unlock(int address) {
		super(address);
	}

	@Override
	public String toString() {
		return "Unlock" + packetDelimiter + address;
	}
}

class Write extends FileOperation {
	String content;
	boolean append;

	public Write(String content, boolean append) {
		this.content = content;
		this.append = append;
	}

	@Override
	public String toString() {
		return "Write" + packetDelimiter + append + packetDelimiter + content;
	}
}
