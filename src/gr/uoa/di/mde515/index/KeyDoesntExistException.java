package gr.uoa.di.mde515.index;


public final class KeyDoesntExistException extends Exception {

	private static final long serialVersionUID = -7595335469841073376L;

	public KeyDoesntExistException(String k) {
		super("Key " + k + " does not exist");
	}
}
