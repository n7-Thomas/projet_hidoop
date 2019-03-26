package exceptions;

/**
 * Erreur intervenant lors de la lecture d'un fichier.
 */
public class ErreurLectureFichierException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	
	public ErreurLectureFichierException(String m){
		super(m);
	}
}
