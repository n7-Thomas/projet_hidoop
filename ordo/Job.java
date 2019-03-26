package ordo;

import java.rmi.Naming;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import config.Project;
import exceptions.ErreurLancementRunMapException;
import exceptions.NomDeFichierInconnuException;
import formats.Format;
import formats.Format.OpenMode;
import formats.Format.Type;
import formats.KVFormat;
import formats.LineFormat;
import hdfs.HdfsClient;
import map.MapReduce;

public class Job implements JobInterfaceX {

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

	/**
	 * Choisir le type du fichier.
	 */
	@Override
	public void setInputFormat(Type ft) {
		inputFormat = ft;
	}

	/**
	 * Choisir le nom du fichier.
	 */
	@Override
	public void setInputFname(String fname) {
		inputFname = fname;
	}
	
	
	/**
	 * Lancer un job à partir d'un map reduce. Crée un FormatReader et un
	 * FormatWriter, lance le map sur chaque daemon Recupère les fichiers et
	 * applique le reduce.
	 * 
	 * @param outputFname
	 * 
	 * @param MapReduce
	 */
	@Override
	public void startJob(MapReduce mr) {
		if (inputFname != null || inputFormat != null) {

			/*
			 * Récupération de la liste des Daemons où sont stockés les fichiers
			 */
			List<String> daemons = HdfsClient.HdfsGetDataNodesOfFile(inputFname).getMain();
			if (daemons == null)
				throw new NomDeFichierInconnuException("Ce fichier n'est pas enregistré dans le Name Node.");
			
		
			Callback cb = null;
			try {
				cb = new Callback(daemons.size());
			} catch (RemoteException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			/*
			 * Pour chaque fragments, on crée un reader/writer et on lance le
			 * map
			 */
			for (int i = 0; i < daemons.size(); i++) {

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
					String url = "//" + daemons.get(i) + ":" + Project.PortDaemon + "/Daemon";

					System.out.println("Requete vers Daemon au " + url);

					// Lancement du RunMap sur le Daemon
					Remote r = Naming.lookup(url);

					if (r instanceof DaemonInterface) {
						System.out.println("RunMap..");
						((DaemonInterface) r).runMap(mr, reader, writer, cb);
					} else {
						throw new ErreurLancementRunMapException("Erreur lors de l'execution du RunMap");
					}

				} catch (Exception e) {
					e.printStackTrace();
					
					System.out.println("Erreur dans le contact au Daemon. Arrêt");
					return ;
				}
			}
			
			// Attendre la fin des runMap	
			try {
				cb.bloquer();
			} catch (RemoteException | InterruptedException e) {
				e.printStackTrace();
			}
						

			// Récupération des fichiers intermédiaires
			System.out.println("Lancement Read Inter : " + inputFname);
			HdfsClient.HdfsReadIntermediaires(inputFname, inputFname + "-prereduce");

			
			// Destruction des fichiers intermédiaires
			//HdfsClient.HdfsDeleteIntermediaire(inputFname);


			// Création des FormatReader/Writer pour le reduce
			Format readerReduce = new KVFormat();
			readerReduce.setPath(Project.PATH + Project.PATH_FILES);
			//readerReduce.setPath("/home/tdarget/nosave/");
			readerReduce.setFname(inputFname + "-prereduce");
			readerReduce.open(OpenMode.R);
			
			
			Format writerReduce = null;
			if (this.outputFormat == Type.KV)
				writerReduce = new KVFormat();
			else
				writerReduce = new LineFormat();

			writerReduce.setPath(Project.PATH + Project.PATH_FILES);
			//writerReduce.setPath("/home/tdarget/nosave/");
			
			if (this.outputFname == null)
				writerReduce.setFname(inputFname + "-resultat");
			else
				writerReduce.setFname(outputFname);

			writerReduce.open(OpenMode.W);

			// Reduce
			mr.reduce(readerReduce, writerReduce);

			
			// Fermeture des streams
			writerReduce.close();
			readerReduce.close();
			
			System.out.println("MapReduce effectué sur " + inputFname + " enregistré ici: " + Project.PATH + Project.PATH_FILES);
			
		} else {
			throw new NomDeFichierInconnuException("Le fichier n'a pas été spécifié.");
		}
	}

	
	/**
	 * Beaucoup de fonctions à implémenter dans les prochaines versions : 
	 */
	@Override
	public void setNumberOfReduces(int tasks) {
		// A implementer
	}

	@Override
	public void setNumberOfMaps(int tasks) {
		// A implementer
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
		// Pour etape 2

	}

	@Override
	public int getNumberOfReduces() {
		// A implementer
		return 0;
	}

	@Override
	public int getNumberOfMaps() {
		// A implementer
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
		// Etape 2
		return null;
	}

}
