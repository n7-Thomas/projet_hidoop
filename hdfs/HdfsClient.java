package hdfs;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.rmi.Naming;
import java.rmi.Remote;
import java.util.List;
import java.util.Map;
import config.Project;
import exceptions.ErreurEnvoiInformationsServeurException;
import exceptions.ErreurRecuperationNameNodeException;
import formats.Format;
import formats.Format.OpenMode;
import formats.Format.Type;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;
import hdfs.Requete.TypeRequete;

public class HdfsClient {

	private static final String urlNameNode = "//" + Project.AdresseNameNode + ":" + Project.PortNameNode + "/NameNode";

	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <line|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
	}

	/**
	 * Retourne les datanodes utilisés pour enregistrer le fichier.
	 * 
	 * @param hdfsFname
	 * @return
	 */
	public static ListesServeurs HdfsGetDataNodesOfFile(String hdfsFname) {

		// Demander au NameNode les datanodes
		ListesServeurs listeDataNode = null;
		try {

			// Regarder sur l'url
			Remote r = Naming.lookup(urlNameNode);
			if (Project.DEBUG)
				System.out.println("Requete vers NameNode au " + urlNameNode);

			// Voir si on a bien reçu
			if (r instanceof NameNodeInterface) {
				listeDataNode = ((NameNodeInterface) r).getlisteDataNode(hdfsFname);
			} else {
				throw new ErreurRecuperationNameNodeException("Erreur du remote");
			}

		} catch (Exception e) {
			throw new ErreurRecuperationNameNodeException("Erreur lors de la récupération des données du NameNode");
		}
		return listeDataNode;
	}

	/**
	 * Effacer un fichier du FileSystem.
	 * 
	 * @param hdfsFname
	 */
	public static void HdfsDelete(String hdfsFname) {
		HdfsClient.HdfsSuppression(hdfsFname, false);
	}

	public static void HdfsDeleteIntermediaire(String hdfsFname) {
		HdfsClient.HdfsSuppression(hdfsFname, true);
	}

	private static Socket connexionVers(ListesServeurs listeDataNode, int i) {
		Socket socketVersDN = null;
		try {
			socketVersDN = new Socket(listeDataNode.getMain().get(i), Project.PortDataNode);
		} catch (IOException e) {
			System.out.println("Pas de connexion vers le serveur : " + listeDataNode.getMain().get(i));
			System.out.println("Tentative de récupération vers le serveur : " + listeDataNode.getSavedFragment(i));
			try {
				socketVersDN = new Socket(listeDataNode.getSavedFragment(i), Project.PortDataNode);
			} catch (IOException e1) {
				System.out.println("Pas de connexion vers le serveur : " + listeDataNode.getSavedFragment(i));
				System.out.println("Echec de récupération du fragment " + i);
				e1.printStackTrace();

			}
		}
		return socketVersDN;
	}

	private static void HdfsSuppression(String hdfsFname, boolean detruireFichiersIntermediaire) {

		ListesServeurs listeDataNode = null;
		try {
			String url = "//" + Project.AdresseNameNode + ":" + Project.PortNameNode + "/NameNode";
			Remote r = Naming.lookup(url);
			if (Project.DEBUG)
				System.out.println("Requete vers NameNode au " + url);
			if (r instanceof NameNodeInterface) {
				listeDataNode = ((NameNodeInterface) r).getlisteDataNode(hdfsFname);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (Project.DEBUG)
			System.out.println("Préparation de la suppression de " + listeDataNode.getMain().size() + " paquets.");

		for (int i = 0; i < listeDataNode.getMain().size(); i++) {

			// Creation de la socket
			Socket socketVersDN = connexionVers(listeDataNode, i);
			
			if (Project.DEBUG)
				System.out.println("Socket créée");

			Requete req = null;
			if (detruireFichiersIntermediaire)
				req = new Requete(TypeRequete.SupprimerIntermediaires, hdfsFname, i, 0);
			else
				req = new Requete(TypeRequete.Supprimer, hdfsFname, i, 0);

			ObjectOutputStream objectOutputStreamVersDN = null;
			try {
				objectOutputStreamVersDN = new ObjectOutputStream(socketVersDN.getOutputStream());
			} catch (IOException e) {
				System.out.println("Erreur creation objectoutputstream");
			}

			if (Project.DEBUG)
				System.out.println("Output stream req créée");
			// Envoi de la requete
			try {
				objectOutputStreamVersDN.writeObject(req);
			} catch (IOException e) {
				System.out.println("Erreur envoi requete");
			}
			try {
				objectOutputStreamVersDN.close();
				socketVersDN.close();
			} catch (IOException e) {
				System.out.println("Erreur fermeture des stream");
			}
		}

		if (!detruireFichiersIntermediaire) {
			/* Supprimer le fichier de l'annuaire */
			try {
				String url = "//" + Project.AdresseNameNode + ":" + Project.PortNameNode + "/NameNode";
				Remote r = Naming.lookup(url);
				if (Project.DEBUG)
					System.out.println("Requete vers NameNode au " + url);
				if (r instanceof NameNodeInterface) {
					((NameNodeInterface) r).supprimer(hdfsFname);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println("Fichier: " + hdfsFname + " supprimé avec succès.");
		} else {
			System.out.println("Fichiers intermediaires de " + hdfsFname + "supprimés avec succcès");
		}
	}

	/**
	 * Ecrire un fichier sur le FileSystem.
	 * 
	 * @param fmt
	 * @param localFSSourceFname
	 * @param repFactor
	 * @throws IOException
	 * @throws ErreurEnvoiInformationsServeurException
	 */
	public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, boolean withSave)
			throws IOException, ErreurEnvoiInformationsServeurException {

		// Recuperer le fichier a envoyer
		Format reader = null;
		if (fmt == Type.LINE)
			reader = new LineFormat();
		else
			reader = new KVFormat();

		reader.setPath(Project.PATH + Project.PATH_FILES);
		// reader.setPath("/home/tdarget/nosave/");
		reader.setFname(localFSSourceFname);

		// Calculer le nombre de paquets a distfribuer
		long tailleFile = reader.getLength();
		int nbPaquets = (int) (tailleFile / Project.NombreKVMax) + 1;

		reader.open(OpenMode.R);

		// Demander au NameNode une repartition des paquets
		ListesServeurs listeDataNode = null;
		try {
			Remote r = Naming.lookup(urlNameNode);
			System.out.println("Requete vers NameNode au " + urlNameNode);
			if (r instanceof NameNodeInterface) {
				((NameNodeInterface) r).allouer(localFSSourceFname, nbPaquets);
				listeDataNode = ((NameNodeInterface) r).getlisteDataNode(localFSSourceFname);
			}
		} catch (Exception e) {
			System.out
					.println("Erreur de connexion au NameNode, vérifiez que le Namenode est lancé sur " + urlNameNode);
			return;
		}

		System.out.println("Préparation de l'envoi de " + nbPaquets + " paquets.");
		envoiVersServeur(nbPaquets, listeDataNode.getMain(), localFSSourceFname, reader);
		
		if(withSave){
			envoiVersServeur(nbPaquets, listeDataNode.getBackup(), localFSSourceFname, reader);
		}
		
		
		
		
		
		System.out.println("Fichier " + localFSSourceFname + " enregistré avec succès.");
	}

	
	private static void envoiVersServeur(int nbPaquets, List<String> listeDataNode, String localFSSourceFname, Format reader) throws IOException{
		
		System.out.println("Préparation de l'envoi de " + nbPaquets + " paquets.");

		for (int i = 0; i < nbPaquets; i++) {

			// Creation de la socket
			Socket socketVersDN = null;
			try {
				socketVersDN = new Socket(listeDataNode.get(i), Project.PortDataNode);
			} catch (IOException e) {
				System.out.println("Pas de connexion vers le serveur : " + listeDataNode.get(i));
			}
			
			if (Project.DEBUG)
				System.out.println("Création de la socket..");


			// Stream Output pour envoi de données
			if (Project.DEBUG)
				System.out.println("Création de l'output stream..");

			ObjectOutputStream objectOutputStreamVersDN = null;
			try {
				objectOutputStreamVersDN = new ObjectOutputStream(socketVersDN.getOutputStream());
				objectOutputStreamVersDN.flush();
			} catch (IOException e) {
				socketVersDN.close();
				System.out.println("Erreur de connexion au Datanode");
				return;
				// throw new ErreurEnvoiInformationsServeurException("Erreur
				// création de l'object output stream");
			}

			// Creation la requete
			Requete req = new Requete(TypeRequete.Ecrire, localFSSourceFname, i, 0);

			// Envoi de la requete
			if (Project.DEBUG)
				System.out.println("Envoi de la requete..");
			try {
				objectOutputStreamVersDN.writeObject(req);
			} catch (IOException e) {
				objectOutputStreamVersDN.close();
				socketVersDN.close();
				System.out.println("Erreur de connexion au Datanode");
				return;
			}
			// Envoi des donnees
			try {
				int k = 0;
				KV kv;
				while ((kv = reader.read()) != null && k < Project.NombreKVMax) {
					k++;
					objectOutputStreamVersDN.writeObject(kv);
				}
				objectOutputStreamVersDN.flush();
			} catch (IOException e) {
				objectOutputStreamVersDN.close();
				socketVersDN.close();
				throw new ErreurEnvoiInformationsServeurException("Erreur envoi des données");
			}

			// Fermeture des stream
			try {
				objectOutputStreamVersDN.close();
				socketVersDN.close();
			} catch (IOException e) {
				throw new ErreurEnvoiInformationsServeurException("Erreur Fermeture des stream");
			}

		}	
	}
	
	
	
	
	
	/**
	 * Permet de récupérer un fichier enregistré sur le serveur
	 * 
	 * @param hdfsFname
	 * @param localFSDestFname
	 * @param lecture
	 *            -> permet de discerner si on veut récupérer un fichier ou un
	 *            fichier traité
	 * @throws ClassNotFoundException
	 */
	public static void HdfsRead(String hdfsFname, String localFSDestFname) {
		System.out.println("Récupération des fichiers " + hdfsFname);
		HdfsClient.recuperation(hdfsFname, localFSDestFname, true);
	}

	public static void HdfsReadIntermediaires(String hdfsFname, String localFSDestFname) {
		System.out.println("Récupération des fichiers intermédiaires" + hdfsFname);
		HdfsClient.recuperation(hdfsFname, localFSDestFname, false);
	}

	private static void recuperation(String hdfsFname, String localFSDestFname, boolean lecture) {

		// Récupération de la liste des datanodes sur lesquels on a enregistré
		// le fichier.
		ListesServeurs listeDataNode = null;
		try {
			String url = "//" + Project.AdresseNameNode + ":" + Project.PortNameNode + "/NameNode";
			Remote r = Naming.lookup(url);

			if (Project.DEBUG)
				System.out.println("Requete vers NameNode au " + url);

			if (r instanceof NameNodeInterface) {
				listeDataNode = ((NameNodeInterface) r).getlisteDataNode(hdfsFname);
			}

		} catch (Exception e) {
			throw new ErreurRecuperationNameNodeException(
					"Erreur lors de la récuperation des informations du Namenode");
		}

		// Gestion d'erreur si fichier inconnu
		if (listeDataNode == null) {
			System.out.println("Ce fichier n'est pas enregistré.");
			return;
		}

		// Préparation du writer qui va enregistrer dans data/ le fichier
		// récupéré
		Format writer = new KVFormat();
		writer.setPath(Project.PATH + Project.PATH_FILES);
		writer.setFname(localFSDestFname);
		writer.open(OpenMode.W);

		if (Project.DEBUG)
			System.out.println("Préparation de la lecture de " + listeDataNode.getMain().size() + " paquets.");

		// Récupération des données.
		for (int i = 0; i < listeDataNode.getMain().size(); i++) {

			// Creation de la socket
			Socket socketVersDN = connexionVers(listeDataNode, i);

			// Creation la requete
			Requete req;
			if (lecture)
				req = new Requete(TypeRequete.Recuperer, hdfsFname, i, 0);
			else
				req = new Requete(TypeRequete.RecupererIntermediaires, hdfsFname, i, 0);

			if (Project.DEBUG)
				System.out.println("Requête envoyée");

			// Envoi de la requete
			ObjectOutputStream objectOutputStreamVersDN = null;
			try {
				objectOutputStreamVersDN = new ObjectOutputStream(socketVersDN.getOutputStream());
				objectOutputStreamVersDN.writeObject(req);
			} catch (IOException e) {
				System.out.println("Erreur envoi requete");
			}

			// Reception des KV
			ObjectInputStream objectInputStreamDepuisDN = null;
			try {
				if (Project.DEBUG)
					System.out.println("Création de l'output stream");
				objectInputStreamDepuisDN = new ObjectInputStream(socketVersDN.getInputStream());

				KV kv;
				while ((kv = (KV) objectInputStreamDepuisDN.readObject()) != null) {
					writer.write(kv);
				}

			} catch (EOFException e) {
				/* A GERER MIEUX QUE CA */
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}

			if (Project.DEBUG)
				System.out.println("Fichier " + i + " reçu");

			// Fermeture des streams
			try {

				objectOutputStreamVersDN.close();
				objectInputStreamDepuisDN.close();
				socketVersDN.close();

			} catch (IOException e) {
				throw new ErreurFermetureStreamException(
						"Erreur lors de la fermeture des streams dans la récupération des fichiers");
			}

		}

		writer.close();

		System.out.println(
				"Fichier " + hdfsFname + " récupéré ici : " + Project.PATH + Project.PATH_FILES + localFSDestFname);

	}

	/**
	 * Afficher les DataNodes.
	 */
	private static void HdfsDataNodes() {
		Map<Integer, String> hm = null;
		try {
			Remote r = Naming.lookup(urlNameNode);

			if (Project.DEBUG)
				System.out.println("Requete vers NameNode à l'url" + urlNameNode);

			if (r instanceof NameNodeInterface) {
				hm = ((NameNodeInterface) r).getDataNodes();
			}
			System.out.println("DataNodes : ");
			System.out.println(hm);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Afficher la carte des fichiers stockés.
	 */
	public static void HdfsCarte() {
		// Demander au NameNode une repartition des paquets
		Map<String, ListesServeurs> hm = null;
		try {
			Remote r = Naming.lookup(urlNameNode);
			if (Project.DEBUG)
				System.out.println("Requete vers NameNode à l'url" + urlNameNode);
			if (r instanceof NameNodeInterface) {
				hm = ((NameNodeInterface) r).cartographie();
			}
			System.out.println("Cartographie : ");
			System.out.println(hm);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// java HdfsClient <read|write> <line|kv> <file>
		/*
		 * char choix = '0'; Scanner sc = new Scanner(System.in);
		 * 
		 * while (choix != 'q') { afficherMenu(); choix =
		 * sc.nextLine().charAt(0); switch (choix) { case '1': HdfsCarte();
		 * break; case '2': HdfsDataNodes(); break; case '3': Format.Type fmt;
		 * if (args.length < 3) { usage(); return; } if (args[1].equals("line"))
		 * fmt = Format.Type.LINE; else if (args[1].equals("kv")) fmt =
		 * Format.Type.KV; else { usage(); return; } HdfsWrite(fmt, args[2], 1);
		 * break; case '4': HdfsDataNodes(); break; case '5': HdfsCarte();
		 * break; case 'q': break; default: System.out.println(
		 * "Erreur dans la commande saisie"); break; } }
		 */

		long t1 = System.currentTimeMillis();
		try {
			if (args.length < 2) {
				usage();
				return;
			}

			switch (args[0]) {
			case "carte":
				HdfsCarte();
				break;
			case "datanodes":
				HdfsDataNodes();
				break;
			case "read":
				HdfsRead(args[1], "resultat.txt");
				break;
			case "delete":
				HdfsDelete(args[1]);
				break;
			case "write":
				Format.Type fmt;
				if (args.length < 3) {
					usage();
					return;
				}
				if (args[1].equals("line"))
					fmt = Format.Type.LINE;
				else if (args[1].equals("kv"))
					fmt = Format.Type.KV;
				else {
					usage();
					return;
				}
				HdfsWrite(fmt, args[2], false);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long t2 = System.currentTimeMillis();
		System.out.println("Tâche exécuté en " + (t2 - t1) / 1000 + "s");
		System.exit(0);
	}

}
