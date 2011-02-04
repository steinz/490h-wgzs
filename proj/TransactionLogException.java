import java.io.IOException;

/**
 * CSE 490h
 * @author wayger, steinz
 */

/**
 * TODO: Think about Exception Hierarchy
 */

public class TransactionLogException extends IOException {

	private static final long serialVersionUID = 4891161698848299644L;

	public TransactionLogException(String string) {
		super(string);
	}
	
}
