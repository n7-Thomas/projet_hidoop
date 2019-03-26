package exceptions;

/**
 * Erreur intervenant lors de l'enregistrement.
 */
public class ErreurEnregistrementException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	
	public ErreurEnregistrementException(String m){
		super(m);
	}
}
