/**
 * CSE 490h
 * @author wayger, steinz
 */

import java.io.IOException;

class FileAlreadyExistsException extends IOException {
	private static final long serialVersionUID = -4672741364887472499L;
}

class NotClientException extends Exception {
	private static final long serialVersionUID = 2823129727550319441L;
}

class NotManagerException extends Exception {
	private static final long serialVersionUID = -2133442099641600446L;

	public NotManagerException() {
		super();
	}

	public NotManagerException(String string) {
		super(string);
	}
}

class PacketPackException extends Exception {
	private static final long serialVersionUID = 5273893683486775453L;

	public PacketPackException(String msg) {
		super(msg);
	}
}

class TransactionException extends Exception {
	private static final long serialVersionUID = -7296103807819087346L;

	public TransactionException(String str) {
		super(str);
	}
}

class TransactionLogException extends IOException {
	private static final long serialVersionUID = 4891161698848299644L;

	public TransactionLogException(String string) {
		super(string);
	}
}

class UnknownManagerException extends Exception {
	private static final long serialVersionUID = 6525390876463186997L;
}
