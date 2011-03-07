package edu.washington.cs.cse490h.tdfs;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
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
	private TDFSNode n;

	private PersistentStorageOutputStream acceptedStream;

	private PersistentStorageOutputStream promisedStream;

	/**
	 * Last propNum promised for (file, opNum)
	 */
	private Map<Tuple<String, Integer>, Integer> promised;

	/**
	 * Accepted ((file, opNum), value) with highest propNum
	 */
	private Map<Tuple<String, Integer>, Proposal> accepted;

	public PersistentPaxosState(TDFSNode n) {
		try {
			this.accepted = new HashMap<Tuple<String, Integer>, Proposal>();
			this.acceptedStream = n.getOutputStream(acceptedLog, true);
			this.n = n;
			this.promised = new HashMap<Tuple<String, Integer>, Integer>();
			this.promisedStream = n.getOutputStream(promisedLog, true);
			this.rebuild();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void accept(Proposal value) {
		accepted.put(new Tuple<String, Integer>(value.filename,
				value.operationNumber), value);
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
	
	public Proposal highestAcceptedProposal(String filename, int operationNumber) {
		return accepted.get(new Tuple<String, Integer>(filename, operationNumber));
	}
	
	public Integer highestPromisedProposalNumber(String filename, int operationNumber) {
		return promised.get(new Tuple<String, Integer>(filename, operationNumber));
	}

	public void promise(String filename, int operationNumber,
			int proposalNumber) {
		promised.put(new Tuple<String, Integer>(filename, operationNumber),
				proposalNumber);
		try {
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(bytes);
			out.writeInt(proposalNumber);
			out.writeInt(operationNumber);
			out.writeInt(2 * filename.length());
			out.writeChars(filename);
			out.close();
			promisedStream.write(bytes.toByteArray());
			promisedStream.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void rebuild() {
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
				int operationNumber = in.readInt();
				int len = in.readInt();
				byte[] bytes = new byte[len];
				if (in.read(bytes) != len) {
					throw new EOFException();
				}
				String filename = new String(bytes);
				promised.put(new Tuple<String, Integer>(filename,
						operationNumber), proposalNumber);
			}
		} catch (EOFException e) {
			// done
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
