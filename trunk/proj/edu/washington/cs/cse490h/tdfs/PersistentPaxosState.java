package edu.washington.cs.cse490h.tdfs;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import edu.washington.cs.cse490h.lib.Utility;

/**
 * CSE 490h
 * 
 * @author wayger, steinz
 */

/**
 * Logs operations to persistent storage for recovery
 */
public class PersistentPaxosState {

	/**
	 * Log filenames
	 */
	protected static final String acceptedLog = "accepted.log",
			promisedLog = "promised.log";

	/**
	 * The node this FS is associated with - used to access the underlying file
	 * system and for logging
	 */
	protected TDFSNode n;

	/**
	 * Log streams
	 */
	protected DataOutputStream acceptedStream, promisedStream;

	/**
	 * Accepted ((file, opNum), value) with highest propNum
	 */
	protected Map<Tuple<String, Integer>, Proposal> accepted;

	/**
	 * Highest propNum promised for (file, opNum)
	 */
	protected Map<Tuple<String, Integer>, Integer> promised;

	public PersistentPaxosState(TDFSNode n) {
		this.accepted = new HashMap<Tuple<String, Integer>, Proposal>();
		this.n = n;
		this.promised = new HashMap<Tuple<String, Integer>, Integer>();

		try {
			this.rebuild();
			this.acceptedStream = new DataOutputStream(n.getOutputStream(
					acceptedLog, true));
			this.promisedStream = new DataOutputStream(n.getOutputStream(
					promisedLog, true));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void accept(Proposal value) {
		accepted.put(new Tuple<String, Integer>(value.filename,
				value.operationNumber), value);
		try {
			byte[] packed = value.pack();
			acceptedStream.writeInt(packed.length);
			acceptedStream.write(packed);
			acceptedStream.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Closes the underlying stream for tests
	 */
	public void close() throws IOException {
		acceptedStream.close();
		promisedStream.close();
	}
	
	public Proposal highestAccepted(String filename, int operationNumber) {
		return accepted.get(new Tuple<String, Integer>(filename,
				operationNumber));
	}

	public Integer highestPromised(String filename, int operationNumber) {
		return promised.get(new Tuple<String, Integer>(filename,
				operationNumber));
	}

	public void promise(String filename, int operationNumber, int proposalNumber) {
		Tuple<String, Integer> key = new Tuple<String, Integer>(filename,
				operationNumber);
		Integer current = promised.get(key);
		if (current != null && current > proposalNumber) {
			return;
		}

		promised.put(key, proposalNumber);
		try {
			promisedStream.writeInt(proposalNumber);
			promisedStream.writeInt(operationNumber);
			byte[] filenameBytes = Utility.stringToByteArray(filename);
			promisedStream.writeInt(filenameBytes.length);
			promisedStream.write(filenameBytes);
			promisedStream.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	protected void rebuild() throws IOException {
		try {
			DataInputStream in = new DataInputStream(n
					.getInputStream(acceptedLog));
			while (true) {
				int len = in.readInt();
				byte[] bytes = new byte[len];
				if (in.read(bytes) != len) {
					throw new EOFException();
				}
				Proposal p = new Proposal(bytes);
				accepted.put(new Tuple<String, Integer>(p.filename,
						p.operationNumber), p);
			}
		} catch (FileNotFoundException e) {
			// nothing to do
		} catch (EOFException e) {
			// done
		}

		try {
			DataInputStream in = new DataInputStream(n
					.getInputStream(promisedLog));
			while (true) {
				int proposalNumber = in.readInt();
				int operationNumber = in.readInt();
				int len = in.readInt();
				byte[] bytes = new byte[len];
				if (in.read(bytes) != len) {
					throw new EOFException();
				}
				String filename = Utility.byteArrayToString(bytes);
				promised.put(new Tuple<String, Integer>(filename,
						operationNumber), proposalNumber);
			}
		} catch (FileNotFoundException e) {
			// nothing to do
		} catch (EOFException e) {
			// done
		}
	}
}
