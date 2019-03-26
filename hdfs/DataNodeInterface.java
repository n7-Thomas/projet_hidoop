package hdfs;

import java.io.ObjectInputStream;


public interface DataNodeInterface {
	/**
	 * Enregistrer un fichier dans l'espace disque
	 */
	boolean enregistrer(String nomFichier, int identifiant, ObjectInputStream ois);
	
	/**
 	* Récupérer un fichier dans l'espace disque
	 */
	boolean recuperer(String nomFichier, int identifiant, boolean lecture);
	
	/**
	 * Supprimer un fichier de l'espace disque
	 */
	boolean supprimer(String nomFichier, int identifiant, boolean intermediaires);	
}
