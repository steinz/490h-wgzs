/**
 * CSE 490h
 * @author wayger, steinz
 */

/**
 * Put all error codes in here
 */
public class ErrorCode {
	// TODO: LOW: Replace this class w/ an enum

	public static final int UnknownError = -1;

	// file system errors
	public static final int FileDoesNotExist = 10;
	public static final int FileAlreadyExists = 11;
	public static final int Timeout = 20;
	public static final int FileTooLarge = 30;

	// cache coherence errors
	public static final int NotManager = 40;
	public static final int MultipleOwners = 41;
	public static final int ReadWriteAndReadOnly = 42;
	public static final int UnknownManager = 45;
	public static final int MissingIntent = 46;

	// onCommand parsing errors
	public static final int InvalidCommand = 900;
	public static final int IncompleteCommand = 901;
	public static final int InvalidServerAddress = 910;
	public static final int DynamicCommandError = 950;

	/**
	 * Returns the string associated with the given error code
	 */
	public static String lookup(int code) {
		switch (code) {
		case 10:
			return "10 FileDoesNotExist";
		case 11:
			return "11 FileAlreadyExists";
		case 20:
			return "20 Timeout";
		case 30:
			return "30 FileTooLarge";
		case 40:
			return "40 NotManager";
		case 41:
			return "41 MultipleOwners";
		case 42:
			return "42 ReadWriteAndReadOnly";
		case 45:
			return "45 UnknownManager";
		case 46:
			return "46 MissingIntent";
		case 900:
			return "900 InvalidCommand";
		case 901:
			return "901 IncompleteCommand";
		case 910:
			return "910 IncompleteServerAddress";
		case 950:
			return "950 DynamicCommandError";
		case -1:
		default:
			return "UnknownError";
		}
	}
}
