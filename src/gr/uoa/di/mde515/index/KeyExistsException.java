package gr.uoa.di.mde515.index;

public final class KeyExistsException extends Exception {

	private static final long serialVersionUID = 720930361671317055L;

	public KeyExistsException(String string) {
		super("Key " + string + " exists");
	}
}
