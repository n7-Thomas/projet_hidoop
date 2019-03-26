package exceptions;

/**
 * Erreur intervenant lorsque un fichier n'est pas trouvé.
 */
public class NomDeFichierInconnuException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	
	public NomDeFichierInconnuException(String m){
		super(m);
	}
}
