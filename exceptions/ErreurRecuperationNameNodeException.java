package exceptions;

/**
 * Erreur intervenant lors de la récupération d'informations envoyés par le Namenode.
 */
public class ErreurRecuperationNameNodeException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	
	public ErreurRecuperationNameNodeException(String m){
		super(m);
	}
}
