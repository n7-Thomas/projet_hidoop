package exceptions;

/**
 * Erreur intervenant lors de la fermeture de stream.
 */
public class ErreurFermetureStreamException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	
	public ErreurFermetureStreamException(String m){
		super(m);
	}
}
