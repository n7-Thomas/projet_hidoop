package ordo;

import java.rmi.Naming;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import config.Project;
import exceptions.ErreurLancementRunMapException;
import exceptions.NomDeFichierInconnuException;
import formats.Format;
import formats.KVFormat;
import formats.LineFormat;
import formats.Format.OpenMode;
import formats.Format.Type;
import hdfs.HdfsClient;
import hdfs.ListesServeurs;
import map.MapReduce;

public class JobX implements JobInterfaceX {
	/**
	 * Type du fichier.
	 */
	private Type inputFormat = Type.KV;

	/**
	 * Nom du fichier sur lequel on bosse.
	 */
	private String inputFname = null;

	/**
	 * Nom du fichier de sortie.
	 */
	private String outputFname = null;

	/**
	 * Format de sortie du fichier.
	 */
	private Type outputFormat = Type.KV;

	@Override
	public void setInputFormat(Type ft) {
		this.inputFormat = ft;
	}

	@Override
	public void setInputFname(String fname) {
		this.inputFname = fname;
	}

	@Override
	public void startJob(MapReduce mr) {
		// Récupérer la liste des serveurs
		ListesServeurs ls = HdfsClient.HdfsGetDataNodesOfFile(inputFname);
		if (ls == null)
			throw new NomDeFichierInconnuException("Ce fichier n'est pas enregistré dans le Name Node.");

		List<String> daemons = ls.getMain();
		List<String> backup = ls.getBackup();
		if (daemons == null)
			throw new NomDeFichierInconnuException("Ce fichier n'est pas enregistré dans le Name Node.");

		if (backup == null)
			System.out.println("Pas de liste de backup");

		// Créer un callback recevant la fin des reduce
		Callback callbackFinReduce = null;
		try {
			callbackFinReduce = new Callback(daemons.size());
		} catch (RemoteException e) {
			System.out.println("Erreur lors de la création du callback de fin des reduces.");
			e.printStackTrace();
			return;
		}

		// Diviser les tâches
		int nbServeursReducersDispo = Project.nomMachineDaemonReducer.length;
		int nbTaches = daemons.size();
		int nbTachesParServeur = nbTaches / nbServeursReducersDispo + 1;

		List<Map<Integer, String>> listeTaches = new ArrayList<Map<Integer, String>>();

		int frag = 0;

		for (int i = 0; i < nbServeursReducersDispo; i++) {
			Map<Integer, String> hm = new HashMap<Integer, String>();
			int j = 0;
			while (frag < nbTaches && j < nbTachesParServeur) {
				hm.put(frag, daemons.get(frag));
				frag++;
				j++;
			}
			listeTaches.add(hm);
		}

		// Envoyer au daemon reduce la liste des fragments à traiter
		for (int i = 0; i < listeTaches.size(); i++) {

			// Lancer sur les daemons
			try {

				// Url du Daemon
				String url = "//" + Project.nomMachineDaemonReducer[i] + ":" + Project.PortDaemonReducer
						+ "/DaemonReducer";

				System.out.println("Requete vers Daemon Reducer au " + url);

				// Lancement du RunMap sur le Daemon
				Remote r = Naming.lookup(url);

				if (r instanceof DaemonReducerInterface) {
					System.out.println("RunMap..");
					((DaemonReducerInterface) r).runMapsAndReduce(mr, listeTaches.get(i), this.inputFname,
							this.outputFname, inputFormat, outputFormat, callbackFinReduce);
				} else {
					throw new ErreurLancementRunMapException("Erreur lors de l'envoi de l'ordre au DaemonReducer");
				}

			} catch (Exception e) {
				e.printStackTrace();

				System.out.println("Erreur dans le contact au Daemon. Arrêt");
				return;
			}

		}

		// Bloquer
		try {
			callbackFinReduce.bloquer();
		} catch (RemoteException | InterruptedException e) {
			e.printStackTrace();
		}

		// Récupérer les fichiers déjà reduce 1 fois

		// Récupération des fichiers intermédiaires
		System.out.println("Lancement Read Inter sur DeamonReducer : " + inputFname);
		HdfsClient.HdfsReadIntermediaires(inputFname, inputFname + "-resultat");

		// Création des FormatReader/Writer pour le reduce
		Format readerReduce = new KVFormat();
		readerReduce.setPath(Project.PATH + Project.PATH_FILES);
		readerReduce.setFname(inputFname + "-prereducefinal");
		readerReduce.open(OpenMode.R);

		Format writerReduce = null;
		if (this.outputFormat == Type.KV)
			writerReduce = new KVFormat();
		else
			writerReduce = new LineFormat();

		writerReduce.setPath(Project.PATH + Project.PATH_FILES);

		if (this.outputFname == null)
			writerReduce.setFname(inputFname + "-resultatfinal");
		else
			writerReduce.setFname(outputFname);

		writerReduce.open(OpenMode.W);

		// Reduce
		mr.reduce(readerReduce, writerReduce);

		// Fermeture des streams
		writerReduce.close();
		readerReduce.close();

		System.out.println(
				"MapReduce effectué sur " + inputFname + " enregistré ici: " + Project.PATH + Project.PATH_FILES);

	}

	@Override
	public void setNumberOfReduces(int tasks) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setNumberOfMaps(int tasks) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setOutputFormat(Type ft) {
		this.outputFormat = ft;
	}

	@Override
	public void setOutputFname(String fname) {
		this.outputFname = fname;
	}

	@Override
	public void setSortComparator(SortComparator sc) {
		// TODO Auto-generated method stub

	}

	@Override
	public int getNumberOfReduces() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getNumberOfMaps() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Type getInputFormat() {
		return this.inputFormat;
	}

	@Override
	public Type getOutputFormat() {
		return this.outputFormat;
	}

	@Override
	public String getInputFname() {
		return this.inputFname;
	}

	@Override
	public String getOutputFname() {
		return this.outputFname;
	}

	@Override
	public SortComparator getSortComparator() {
		// TODO Auto-generated method stub
		return null;
	}

}
