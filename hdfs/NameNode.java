package hdfs;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import config.Project;

public class NameNode extends UnicastRemoteObject implements NameNodeInterface {

	/**
	 * SerialVersionUID.
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Liste des Data Node. Map <Numero, Nom de la Machine>
	 */
	private Map<Integer, String> datanodes;

	/**
	 * Liste des fichiers. Map <NomFichier, List<NomMachine>>
	 */
	private Map<String, ListesServeurs> carte;

	/**
	 * Un NameNode est un annuaire des fichiers, il sait où sont stockés les
	 * fichiers sur l'HDFS.
	 * 
	 * @throws RemoteException
	 */
	public NameNode() throws RemoteException {
		super();
		datanodes = new HashMap<Integer, String>();
		carte = new HashMap<String, ListesServeurs>();
		for (int i = 0; i < Project.NombreServeursUtiles; i++) {
			datanodes.put(i, Project.nomMachinesDataNodes[i]);
		}
	}

	/**
	 * Renvoie le nom du serveur numéro.
	 */
	@Override
	public String getDataNode(int numero) throws RemoteException {
		return datanodes.get(numero);
	}

	/**
	 * Renvoie une liste des datanodes sur lesquels sont stockés un fichier.
	 */
	@Override
	public ListesServeurs getlisteDataNode(String nomFichier) throws RemoteException {
		System.out.println("Demande de " + nomFichier);
		return carte.get(nomFichier);
	}

	/**
	 * Enregistre un fichier dans l'annuaire.
	 */
	@Override
	public void allouer(String nomFichier, int nombrePaquets) throws RemoteException {
		System.out.println("Allocation de " + nombrePaquets + " pour le fichier " + nomFichier);
		List<String> nv = new ArrayList<String>();
		List<String> backup = new ArrayList<String>();
		for (int i = 0; i < nombrePaquets; i++) {
			nv.add(datanodes.get(i % datanodes.size()));
			backup.add(datanodes.get((i + 1) % datanodes.size()));
		}
		ListesServeurs ls = new ListesServeurs();
		ls.setBackup(backup);
		ls.setMain(nv);
		carte.put(nomFichier, ls);
	}

	/**
	 * Renvoie la cartographie (l'ensemble des fichiers et là où ils sont
	 * enregistrés.
	 */
	@Override
	public Map<String, ListesServeurs> cartographie() throws RemoteException {
		return this.carte;
	}

	/**
	 * Renvoie la liste des DataNodes disponibles.
	 */
	@Override
	public Map<Integer, String> getDataNodes() throws RemoteException {
		return this.datanodes;
	}

	/**
	 * Supprime un fichier de l'annuaire.
	 */
	@Override
	public boolean supprimer(String nomFichier) throws RemoteException {
		return (carte.remove(nomFichier) == null);
	}

	/**
	 * Lance un serveur RMI disponible sur l'adresse donné dans les paramètres
	 * du projet.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			LocateRegistry.createRegistry(Project.PortNameNode);
			Naming.rebind("//" + Project.AdresseNameNode + ":" + Project.PortNameNode + "/NameNode", new NameNode());
			System.out.println("Serveur lancé sur le pc : " + Project.AdresseNameNode + ":" + Project.PortNameNode);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}

}
