package ordo;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import config.Project;
import exceptions.ErreurLancementRunMapException;
import formats.Format;
import formats.Format.OpenMode;
import formats.Format.Type;
import hdfs.HdfsClient;
import formats.KVFormat;
import formats.LineFormat;
import map.MapReduce;

public class DaemonReducer extends UnicastRemoteObject implements DaemonReducerInterface {

	private static final long serialVersionUID = 5125910485952263437L;

	/**
	 * Pool d'ouvriers qui lanceront les map
	 */
	private static ExecutorService pool;

	protected DaemonReducer() throws RemoteException {
		super();
	}

	@Override
	public void runMapsAndReduce(MapReduce m, Map<Integer, String> serveurs, String inputFname, String outputFname,
			Type inputFormat, Type outputFormat, Callback callbackJob) throws RemoteException {
		System.out.println("Réception d'une nouvelle tâche");
		pool.execute(new TraiterRunMapReducer(m, serveurs, inputFname, outputFname, inputFormat, outputFormat, callbackJob));
	}

	/**
	 * Création du Serveur RMI Daemon sur la machine courante.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			// Création du registre, lancement du serveur
			LocateRegistry.createRegistry(Project.PortDaemonReducer);
			String url = "//localhost:" + Project.PortDaemonReducer + "/DaemonReducer";
			Naming.rebind(url, new DaemonReducer());

			pool = Executors.newFixedThreadPool(Project.NombreOuvriersParDaemons);
			
			System.out.println("Lancement du Daemon Reducer sur le port : " + Project.PortDaemonReducer);
			System.out.println("En attente..");
		} catch (RemoteException e) {
			e.printStackTrace();
			System.out.println("Erreur création du RMI (RemoteException), arrêt.");
		} catch (MalformedURLException e) {
			e.printStackTrace();
			System.out.println("Erreur création du RMI (MalformedURL), arrêt.");
		}
	}

}

class TraiterRunMapReducer implements Runnable {
	private MapReduce m;
	private Map<Integer, String> serveurs;
	private String inputFname;
	private String outputFname;
	private Type inputFormat;
	private Type outputFormat;
	private Callback callbackJob;

	public TraiterRunMapReducer(MapReduce m, Map<Integer, String> serveurs, String inputFname, String outputFname,
			Type inputFormat, Type outputFormat, Callback callbackJob) {

		this.m = m;
		this.serveurs = serveurs;
		this.inputFname = inputFname;
		this.outputFname = outputFname;
		this.inputFormat = inputFormat;
		this.outputFormat = outputFormat;
		this.callbackJob = callbackJob;
	}

	@Override
	public void run() {
		
		System.out.println("Début du run");
		
		// Créer un callback
		Callback cb = null;
		try {
			cb = new Callback(serveurs.size());
		} catch (RemoteException e1) {
			e1.printStackTrace();
		}

		Iterator<Entry<Integer, String>> it = serveurs.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Integer, String> paire = (Map.Entry<Integer, String>) it.next();
			int i = paire.getKey();
			String serveur = paire.getValue();

			// Format Reader
			Format reader = null;
			switch (inputFormat) {
			case KV:
				reader = new KVFormat();
				break;
			case LINE: // Les fichiers sont déjà enregistrés au format KV
				reader = new KVFormat();
				break;
			}
			reader.setFname(inputFname.substring(0, inputFname.length() - 4) + i + ".txt");

			// Format Writer
			Format writer = new KVFormat();
			writer.setFname(inputFname.substring(0, inputFname.length() - 4) + i + "-res.txt");

			// Lancer sur les daemons
			try {

				// Url du Daemon
				String url = "//" + serveur + ":" + Project.PortDaemon + "/Daemon";

				System.out.println("Requete vers Daemon au " + url);

				// Lancement du RunMap sur le Daemon
				Remote r = Naming.lookup(url);

				if (r instanceof DaemonInterface) {
					System.out.println("RunMap..");
					((DaemonInterface) r).runMap(m, reader, writer, cb);
				} else {
					throw new ErreurLancementRunMapException("Erreur lors de l'execution du RunMap");
				}

			} catch (Exception e) {
				e.printStackTrace();

				System.out.println("Erreur dans le contact au Daemon. Arrêt");
				return;
			}
		}

		// On bloque
		try {
			cb.bloquer();
		} catch (RemoteException | InterruptedException e) {
			e.printStackTrace();
		}

		// On reduce
		// Récupération des fichiers intermédiaires
		System.out.println("Lancement Read Inter : " + inputFname);
		HdfsClient.HdfsReadIntermediaires(inputFname, inputFname + "-prereduce");

		// Destruction des fichiers intermédiaires
		// HdfsClient.HdfsDeleteIntermediaire(inputFname);

		// Création des FormatReader/Writer pour le reduce
		Format readerReduce = new KVFormat();
		readerReduce.setPath(Project.PATH + Project.PATH_FILES);
		readerReduce.setFname(inputFname + "-prereduce");
		readerReduce.open(OpenMode.R);

		Format writerReduce = null;
		if (outputFormat == Type.KV)
			writerReduce = new KVFormat();
		else
			writerReduce = new LineFormat();

		writerReduce.setPath(Project.PATH + Project.PATH_FILES);

		if (outputFname == null)
			writerReduce.setFname(inputFname + "-resultat");
		else
			writerReduce.setFname(outputFname);

		writerReduce.open(OpenMode.W);

		// Reduce
		m.reduce(readerReduce, writerReduce);

		// Fermeture des streams
		writerReduce.close();
		readerReduce.close();

		// On débloque le callback
		try {
			callbackJob.finTache();
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

}
