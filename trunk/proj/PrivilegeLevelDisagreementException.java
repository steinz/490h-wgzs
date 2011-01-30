/**
 * CSE 490h
 * 
 * @author wayger, steinz
 */

/*
 * TODO: Have clients and managers verify expected privilege levels and throw
 * this exception on disagreements
 */

public class PrivilegeLevelDisagreementException extends Exception {

	public PrivilegeLevelDisagreementException(String string) {
		super(string);
	}

	private static final long serialVersionUID = 6773974485370472763L;

}
