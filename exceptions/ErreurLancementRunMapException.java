package exceptions;

/**
 * Erreur intervenant lors d'un run map.
 */
public class ErreurLancementRunMapException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	
	public ErreurLancementRunMapException(String m){
		super(m);
	}
}
