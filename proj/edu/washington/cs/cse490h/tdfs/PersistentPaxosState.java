package edu.washington.cs.cse490h.tdfs;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Map;

import edu.washington.cs.cse490h.lib.PersistentStorageOutputStream;

/**
 * CSE 490h
 * 
 * @author wayger, steinz
 */

/**
 * Logs operations to persistent storage for recovery
 */
public class PersistentPaxosState {

	private static final String acceptedLog = "accepted.log";

	private static final String promisedLog = "promised.log";

	/**
	 * The node this FS is associated with - used to access the underlying file
	 * system and for logging
	 */
	protected TDFSNode n;

	protected PersistentStorageOutputStream acceptedStream;

	protected PersistentStorageOutputStream promisedStream;

	/**
	 * Last proposal number promised for a given file
	 */
	private Map<String, Integer> promised;

	/**
	 * filename -> (opNum -> acceptedVal)
	 */
	private Map<String, Proposal> accepted;

	public PersistentPaxosState(TDFSNode n) {
		try {
			this.n = n;
			this.acceptedStream = n.getOutputStream(acceptedLog, true);
			this.promisedStream = n.getOutputStream(promisedLog, true);
			this.rebuild();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	protected void accept(Proposal value) {
		accepted.put(value.filename, value);
		try {
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(bytes);
			byte[] packed = value.pack();
			out.writeInt(packed.length);
			out.write(packed);
			out.close();
			acceptedStream.write(bytes.toByteArray());
			acceptedStream.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	protected void promise(String filename, int proposalNumber) {
		promised.put(filename, proposalNumber);
		try {
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(bytes);
			out.writeInt(proposalNumber);
			out.writeInt(2 * filename.length());
			out.writeChars(filename);
			out.close();
			promisedStream.write(bytes.toByteArray());
			promisedStream.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	protected void rebuild() {
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
				accepted.put(p.filename, p);
			}
		} catch (EOFException e) {
			// done
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		try {
			DataInputStream in = new DataInputStream(n
					.getInputStream(promisedLog));
			while (true) {
				int proposalNumber = in.readInt();
				int len = in.readInt();
				byte[] bytes = new byte[len];
				if (in.read(bytes) != len) {
					throw new EOFException();
				}
				String filename = new String(bytes);
				promised.put(filename, proposalNumber);
			}
		} catch (EOFException e) {
			// done
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
