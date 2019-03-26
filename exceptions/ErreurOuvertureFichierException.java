package exceptions;

/**
 * Erreur intervenant lors de l'ouverture d'un fichier.
 */
public class ErreurOuvertureFichierException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	
	public ErreurOuvertureFichierException(String m){
		super(m);
	}
}
