package exceptions;

/**
 * Erreur intervenant lors de la récupération d'un fragment.
 */
public class ErreurRecuperationFragmentException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	
	public ErreurRecuperationFragmentException(String m){
		super(m);
	}
}
