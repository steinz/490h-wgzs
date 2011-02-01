/**
 * CSE 490h
 * @author wayger, steinz
 */

/**
 * PerfectInitializedClient that logs NEW OPERATION events for Synoptic every
 * time onCommand is called. Will not produce expected output for concurrent
 * commands.
 */
public class PerfectInitializedSynopticLoggingClient extends
		PerfectInitializedClient {

	@Override
	public void onCommand(String line) {
		logSynopticEvent("NEW OPERATION");
		super.onCommand(line);
	}

}
