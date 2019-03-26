package exceptions;

/**
 * Erreur lors de suppression d'un fragment.
 */
public class ErreurSuppressionFragmentException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	
	public ErreurSuppressionFragmentException(String m){
		super(m);
	}
}
