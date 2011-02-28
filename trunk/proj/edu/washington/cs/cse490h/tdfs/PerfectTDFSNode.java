package edu.washington.cs.cse490h.tdfs;

import edu.washington.cs.cse490h.tdfs.TDFSNode;
/**
 * CSE 490h
 * @author wayger, steinz
 */

/**
 * An ideal clone of client that never fails, drop packets, etc.
 * for testing purposes
 */
public class PerfectTDFSNode extends TDFSNode {
    public static double getFailureRate() { return 0; }
    public static double getRecoveryRate() { return 0; }
    public static double getDropRate() { return 0; }
    public static double getDelayRate() { return 0; }
}