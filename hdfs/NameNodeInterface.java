package hdfs;

import java.util.Map;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface NameNodeInterface extends Remote{
	/**
	 *  Retourner un data node par son numero.
	 */
	String getDataNode(int numero) throws RemoteException;
	
	/**
	 *  Retourner la liste des Data Node stockant le fichier.
	 */
	ListesServeurs getlisteDataNode(String nomFichier) throws RemoteException;
	
	/**
	 * Supprimer un fichier.
	 */
	boolean supprimer(String nomFichier) throws RemoteException;
	
	/**
	 * Allouer un nouveau fichier.
	 */
	void allouer(String nomFichier, int nombrePaquets, boolean withSave) throws RemoteException;
	
	/**
	 * Renvoyer les datanodes.
	 */
	Map<Integer, String> getDataNodes() throws RemoteException;
	
	/**
	 * Renvoyer la cartographie
	 */
	Map<String, ListesServeurs> cartographie() throws RemoteException;
	
}
