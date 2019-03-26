package ordo;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import config.Project;
import formats.Format;
import formats.Format.OpenMode;
import map.Mapper;

/**
 * Serveur RMI, qui lancent les maps sur les fichiers stockés sur la même
 * machine. Tournent sur les mêms machines que les Datanodes, mais permettent au
 * service hidoop de fonctionner.
 */
public class Daemon extends UnicastRemoteObject implements DaemonInterface {
	private static final long serialVersionUID = 1L;

	/**
	 * Pool d'ouvriers qui lanceront les map
	 */
	private static ExecutorService pool;

	public Daemon() throws RemoteException {
		super();
	}

	/**
	 * Ajouter un Map dans le pool à executer par RMI. Lancé par un Job.
	 * 
	 * @param Mapper
	 *            m
	 * @param Format
	 *            reader
	 * @param Format
	 *            writer
	 * @param Callback
	 *            cb
	 */
	@Override
	public void runMap(Mapper m, Format reader, Format writer, CallbackInterface cb) throws RemoteException {

		// Lancement du map
		System.out.println("Lancement du Map sur la machine");
		pool.execute(new TraiterRunMap(m, reader, writer, cb));

	}

	/**
	 * Création du Serveur RMI Daemon sur la machine courante.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			// Création du registre, lancement du serveur
			LocateRegistry.createRegistry(Project.PortDaemon);
			String url = "//localhost:" + Project.PortDaemon + "/Daemon";
			Naming.rebind(url, new Daemon());

			System.out.println("Lancement du Daemon sur le port : " + Project.PortDaemon);

			// Création de la pool d'ouvrier pour les map
			// Pour l'instant fixe, potentiellement variable suivant d'autres
			// paramètres
			System.out.println("Création d'une pool de " + Project.NombreOuvriersParDaemons + " ouvriers.");
			pool = Executors.newFixedThreadPool(Project.NombreOuvriersParDaemons);

		} catch (RemoteException e) {
			e.printStackTrace();
			System.out.println("Erreur création du RMI (RemoteException), arrêt.");
		} catch (MalformedURLException e) {
			e.printStackTrace();
			System.out.println("Erreur création du RMI (MalformedURL), arrêt.");
		}
	}

}

/**
 * Classe qui lance un runMap sur un Daemon (serveur RMI). Runnable car sera
 * lancée dans un pool de thread.
 */
class TraiterRunMap implements Runnable {
	// Paramètres passés lors de l'execution du Thread
	private Mapper m;
	private Format reader;
	private Format writer;
	private CallbackInterface cb;

	TraiterRunMap(Mapper m, Format reader, Format writer, CallbackInterface cb) {
		this.m = m;
		this.reader = reader;
		this.writer = writer;
		this.cb = cb;
	}

	@Override
	public void run() {
		// Ouverture des fichiers d'entrée et de sortie
		System.out.println("Ouverture des fichiers");
		reader.open(OpenMode.R);
		writer.open(OpenMode.W);

		// Lancement du map
		System.out.println("Map lancé");
		m.map(reader, writer);

		// Fermeture des fichiers
		System.out.println("Fermeture des fichiers");
		reader.close();
		writer.close();

		// Envoi de l'information au callback
		try {
			cb.finTache();
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		System.out.println("Envoi au callback du signal -> Fin du traitement");

	}

}