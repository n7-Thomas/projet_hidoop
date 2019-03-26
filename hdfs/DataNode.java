package hdfs;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import config.Project;
import exceptions.ErreurEcritureFichierException;
import exceptions.ErreurEnregistrementException;
import exceptions.ErreurRecuperationFragmentException;
import exceptions.ErreurSuppressionFragmentException;
import formats.KV;
import formats.KVFormat;
import formats.Format.OpenMode;

/**
 * Classe gérant un DataNode. Un serveur lancé sur une machine qui stocke des
 * fragments de fichier.
 */
public class DataNode extends Thread implements DataNodeInterface {

	/**
	 * Système de fichier, associant les noms des fichiers aux path des
	 * fragments identifié par leurs identifiants.
	 */
	private static Map<String, Map<Integer, String>> fileSystem = new HashMap<String, Map<Integer, String>>();

	/**
	 * Socket pour transmission avec l'HDFSClient.
	 */
	private Socket s;

	/**
	 * Construire un DataNode à partir de la socket.
	 * 
	 * @param s
	 */
	public DataNode(Socket s) {
		this.s = s;
	}

	
	
	
	
	/**
	 * Enregistrer un fichier sur le Hdfs.
	 * 
	 * @param String
	 *            nom du fichier
	 * @param Int
	 *            identifiant du fichier
	 * @param ObjectInputStream
	 *            utilisé à la réception de la requete et pour recevoir les KV
	 */
	@Override
	public boolean enregistrer(String nomFichier, int identifiant, ObjectInputStream objectInputStreamFromHdfs) {

		// Url d'enregistrement
		String path = Project.PATHfichiersSurDisque + nomFichier.substring(0, nomFichier.length() - 4) + identifiant
				+ ".txt";

		if (Project.DEBUG)
			System.out.println("Creation de l'input stream depuis l'hdfs ");

		// Stream de sortie
		KVFormat writer = null;

		// Initialisation du writer sur le disque
		writer = new KVFormat();
		writer.setFname(nomFichier.substring(0, nomFichier.length() - 4) + identifiant + ".txt");
		writer.open(OpenMode.W);

		if (Project.DEBUG)
			System.out.println("Creation de l'output stream vers le paquet stocké");

		// Reception de l'outputstream
		try {
			KV kv;
			while ((kv = (KV) objectInputStreamFromHdfs.readObject()) != null) {
				writer.write(kv);
			}
		} catch (ErreurEcritureFichierException e) {
			e.printStackTrace();
			throw new ErreurEnregistrementException("Erreur téléchargement et téléversement");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new ErreurEnregistrementException("Erreur téléchargement et téléversement");
		} catch (EOFException e) {
			/*
			 * La fin du fichier lève une EOFException !! On passe outre mais à
			 * changer
			 */
			// throw new ErreurEnregistrementException("Erreur téléchargement et
			// téléversement");
		} catch (IOException e) {
			e.printStackTrace();
			throw new ErreurEnregistrementException("Erreur téléchargement et téléversement");
		}

		if (Project.DEBUG)
			System.out.println("Reception du fichier");

		writer.close();

		if (!fileSystem.containsKey(nomFichier)) {
			fileSystem.put(nomFichier, new HashMap<Integer, String>());
		}
		fileSystem.get(nomFichier).put(identifiant, path);

		System.out.println("Fichier " + writer.getFname() + " ajouté.");

		return true;
	}

	/**
	 * Récupération des fragments d'un fichier, ou des fragments intermédiaires
	 * si boolean lecture à true
	 */
	@Override
	public boolean recuperer(String nomFichier, int identifiant, boolean lecture) {
		if (Project.DEBUG)
			System.out.println("Creation de l'output stream vers le paquet stocké");

		// Création de l'object output stream
		ObjectOutputStream outputStreamVersHDFS = null;
		try {
			outputStreamVersHDFS = new ObjectOutputStream(s.getOutputStream());
		} catch (IOException e) {
			throw new ErreurRecuperationFragmentException("Erreur création d'un outputstream");
		}

		// Récupérer l'adresse du fichier à envoyer
		if (!fileSystem.containsKey(nomFichier)){
			System.out.println("Fichier non trouvé dans le FS : " + fileSystem + nomFichier);
			return false;
		}
		Map<Integer, String> m = fileSystem.get(nomFichier);

		if (!m.containsKey(identifiant)){
			System.out.println("Fragment non trouvé dans le FS : "  + fileSystem + identifiant);
			return false;
		}
		String path = m.get(identifiant);

		// Creer un reader
		KVFormat reader = new KVFormat();
		
		int co = 1;
		if (identifiant > 9){
			co = 2;
		}
		if (identifiant > 99){
			co = 3;
		}
		if (identifiant > 999){
			co = 4;
		}
		
		
		String pathFichier = path.substring(0, path.length() - (nomFichier.length() + co));
		String Fname = nomFichier.substring(0, nomFichier.length() - 4) + identifiant;

		if (!lecture)
			Fname += "-res.txt";
		else
			Fname += ".txt";

		reader.setPath(pathFichier);
		reader.setFname(Fname);
		reader.open(OpenMode.R);
		KV kv;

		if (Project.DEBUG)
			System.out.println("Envoi du fichier : " + pathFichier + Fname);
		
		try {
			while ((kv = reader.read()) != null)
				outputStreamVersHDFS.writeObject(kv);

		} catch (IOException e1) {
			throw new ErreurRecuperationFragmentException("Erreur transfert des données");
		}

		try {
			reader.close();
			outputStreamVersHDFS.close();
		} catch (IOException e) {
			throw new ErreurRecuperationFragmentException("Erreur fermeture des stream");
		}

		return true;
	}

	/**
	 * Suppression d'un fichier, ou du fichiers intermédiaires si boolean
	 * intermediaires true
	 */
	@Override
	public boolean supprimer(String nomFichier, int identifiant, boolean intermediaires) {
		// Gestion d'erreur
		if (nomFichier != "" | nomFichier != null | identifiant >= 0) {

			// On verifie qu'on a bien le fichier sauvegardé
			if (!fileSystem.containsKey(nomFichier))
				return false;
			
			Map<Integer, String> m = fileSystem.get(nomFichier);

			if (!m.containsKey(identifiant))
				return false;

			// On recupère le fichier (si intermediaires alors on cherche les
			// -res)
			String path = m.get(identifiant);
			if (intermediaires)
				path = m.get(identifiant).substring(0, path.length() - 4) + "-res.txt";
			System.out.println(path);
			
			// Suppression du fichier s'il existe
			File fichier = new File(path);
			if (fichier.exists())
				fichier.delete();
			else
				return false;

			if (!intermediaires) {
				m.remove(identifiant);

				if (m.isEmpty())
					fileSystem.remove(nomFichier);
			}

			return true;
		} else {
			throw new ErreurSuppressionFragmentException("Arguments invalides");
		}

	}

	/**
	 * Lancement d'un DataNode, serveur TCP
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		@SuppressWarnings("resource")
		ServerSocket ss = new ServerSocket(Project.PortDataNode);
		System.out.println("Lancement de la Datanode sur le port : " + Project.PortDataNode);
		while (true) {
			System.out.println("En attente de requêtes..");
			new Thread(new DataNode(ss.accept())).start();
		}
	}

	/**
	 * Création d'un thread lors de la reception de requêtes
	 */
	public void run() {
		ObjectInputStream reqInputStream = null;
		System.out.println("Création et reception des requetes");
		try {
			// Reception d'une requete
			reqInputStream = new ObjectInputStream(s.getInputStream());

			Requete req = (Requete) reqInputStream.readObject();
			System.out.println("Réception d'une requete");

			if (req != null) {
				switch (req.getType()) {
				case Ecrire:
					if(!enregistrer(req.getNomFichier(), req.getIdentifiantFichier(), reqInputStream)) System.out.println("Erreur lors de l'enregistrement des fichiers");
					break;
				case Recuperer:
					if(!recuperer(req.getNomFichier(), req.getIdentifiantFichier(), true)) System.out.println("Erreur lors de la récupération des fichiers");
					break;
				case Supprimer:
					if(!supprimer(req.getNomFichier(), req.getIdentifiantFichier(), true)) System.out.println("Erreur lors de la suppression des fichiers intermédiaires");
					if(!supprimer(req.getNomFichier(), req.getIdentifiantFichier(), false)) System.out.println("Erreur lors de la suppression des fichiers");
					break;
				case RecupererIntermediaires:
					if(!recuperer(req.getNomFichier(), req.getIdentifiantFichier(), false)){ System.out.println("Erreur lors de la récupération des fichiers intermédiaires : " + req.getIdentifiantFichier()); Thread.sleep(10000); };
					break;
				case SupprimerIntermediaires:
					if(!supprimer(req.getNomFichier(), req.getIdentifiantFichier(), true)) System.out.println("Erreur lors de la suppression des fichiers intermédiaires");
					break;
				}
			} else {
				if (Project.DEBUG)
					System.out.println("Echec reception requete");
			}
			System.out.println("Fin de tâche");
			reqInputStream.close();
			s.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
