public class ErrorCode {
        // ALL CAPS FOR ERROR CODE NAMES?

	// file system errors
	public static final int FileDoesNotExist = 10;
	public static final int FileAlreadyExists = 11;
	public static final int Timeout = 20;
	public static final int FileTooLarge = 30;
	
	// onCommand parsing errors
	public static final int InvalidCommand =  900;
	public static final int IncompleteCommand = 901;
	public static final int InvalidServerAddress = 910;

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
        case 900:
            return "900 InvalidCommand";
        case 901:
            return "901 IncompleteCommand";
        case 910:
            return "910 IncompleteServerAddress";
        default:
            return "UnknownError";
        }
    }
}
