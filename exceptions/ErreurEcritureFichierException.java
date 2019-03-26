package exceptions;

/**
 * Erreur intervenant lors de l'écriture d'un fichier.
 */
public class ErreurEcritureFichierException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	
	public ErreurEcritureFichierException(String m){
		super(m);
	}
}
