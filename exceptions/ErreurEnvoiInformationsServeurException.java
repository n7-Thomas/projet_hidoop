package exceptions;

/**
 * Erreur intervenant lors de l'écriture sur un serveur.
 */
public class ErreurEnvoiInformationsServeurException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	
	public ErreurEnvoiInformationsServeurException(String m){
		super(m);
	}
}
