package edu.washington.cs.cse490h.tdfs;

import java.io.Serializable;

// TODO: Contention friendly ops

abstract class Operation {
	abstract byte[] pack();
	static abstract Operation unpack(byte[] bytes);
}

class FileOperation extends Operation {
}

class MemberOperation extends Operation {
	int address;

	public MemberOperation(int address) {
		this.address = address;
	}
}

class Create extends FileOperation {
}

class Delete extends FileOperation {
}

/**
 * no need to actually log these
 */
class Get extends FileOperation {
}

class Join extends MemberOperation {
	public Join(int address) {
		super(address);
	}
}

class Leave extends MemberOperation {
	public Leave(int address) {
		super(address);
	}
}

class Lock extends MemberOperation {
	public Lock(int address) {
		super(address);
	}
}

class TXAbort extends Operation {
}

class TXCommit extends Operation {
}

class TXStart extends Operation {
}

class Write extends FileOperation {
	String content;
	boolean append;

	public Write(String content, boolean append) {
		this.content = content;
		this.append = append;
	}
}
