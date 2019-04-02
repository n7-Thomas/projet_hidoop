

import java.io.IOException;
import java.util.Scanner;

import application.MapReduceFreqLetter;
import application.MapReduceIndiceCoincidence;
import application.MapReduceKnuth;
import application.MapReducePi;
import application.MyMapReduce;
import config.Project;
import exceptions.ErreurEnvoiInformationsServeurException;
import formats.Format;
import formats.Format.Type;
import hdfs.HdfsClient;
import hdfs.ListesServeurs;
import map.MapReduce;
import ordo.Job;
import ordo.JobInterfaceX;
import ordo.JobX;


public class Shell {

	static Scanner sc;

	
	/**
	 * Lance un Shell pour pouvoir naviger sur les services.
	 * @param args
	 */
	public static void main(String[] args) {
		sc = new Scanner(System.in);
		char c = '0';

		while (c != 'q') {
			afficherMenu();
			String rep = sc.nextLine();
			c = rep.charAt(0);
			switch (c) {
			case '1':
				hdfs1();
				break;
			case '2':
				hdfs2();
				break;
			case '3':
				hdfs3();
				break;
			case '4':
				hdfs4();
				break;
			case '5':
				hdfs5();
				break;
			case '6':
				hidoop(false);
				break;	
				
			case '7':
				hidoop(true);
				break;	
			
			case 'q':
				break;
			default:
				help();
				break;
			}
			if (c != 'q'){
				System.out.println("\nAppuyez sur entrée..");
				sc.nextLine();
			}
		}
	}

	/**
	 * Fonction d'affichage des possibilités.
	 */
	private static void help() {
		System.out.println("Ce shell permet d'utiliser les 2 services crées Hidoop et HDFS");
		System.out.println("Il est intéractif vous pourrez rentrer les noms de fichiers que vous voulez traiter");
		System.out.println("Veillez à bien avoir lancé les différents services avant: ");
		System.out.println("- NameNode sur " + Project.AdresseNameNode);
		System.out.println("- DataNode et Daemon sur " + Project.nomMachinesDataNodes);	
	}

	/**
	 * Afficher le menu des actions possibles.
	 */
	private static void afficherMenu() {
		System.out.println("Shell HDFS/Hidoop v0");
		System.out.println("Veuillez faire un choix :");
		System.out.println("1. HDFS: Enregistrer un fichier");
		System.out.println("2. HDFS: Recuperer un fichier");
		System.out.println("3. HDFS: Supprimer un fichier");
		System.out.println("4. HDFS: Afficher les fragments d'un fichier déjà enregistré");
		System.out.println("5. HDFS: Afficher les fichiers déjà enregistrés");
		System.out.println("6. Hidoop: Lancer un map-reduce");
		System.out.println("7. Hidoop: Lancer un map-reduce avec reduce multiple");
		System.out.println("h. Aide");
		System.out.println("q. Quitter");
		System.out.print("Votre choix : ");
	}

	/**
	 * Enregistrer un fichier.
	 */
	private static void hdfs1() {
		System.out.println("Le fichier doit être dans hidoop/data");
		System.out.print("Nom du fichier : ");
		String nf = sc.nextLine();
		System.out.print("Format du fichier (kv/line) : ");
		String format = sc.nextLine();
		Type ft;
		if (format.equals("kv"))
			ft = Type.KV;
		else // En cas de mauvaise rentrée on suppose que c'est une Line
			ft = Type.LINE;

		System.out.print("Avec backup y/n: ");
		boolean withBackup = sc.nextLine().contains("y");
		//System.out.println(withBackup);
		try {
			HdfsClient.HdfsWrite(ft, nf, withBackup);
		} catch (ErreurEnvoiInformationsServeurException | IOException e) {
			System.out.println("Une erreur a été levée, veuillez réessayer (nom du fichier pas bon?) ");
		}

	}

	/**
	 * Récupérer un fichier enregistré.
	 */
	private static void hdfs2() {
		System.out.println("Quel fichier voulez vous recuperer? ");
		System.out.print("Nom du fichier enregistré: ");
		String nf = sc.nextLine();
		System.out.print("Nom du fichier destination : ");
		String nfdest = sc.nextLine();

		try {
			HdfsClient.HdfsRead(nf, nfdest);
		} catch (ErreurEnvoiInformationsServeurException e) {
			System.out.println("Une erreur a été levé, veuillez réessayer (nom du fichier pas bon?) ");
		}
	}

	/**
	 * Supprimer un fichier enregistré.
	 */
	private static void hdfs3() {
		System.out.println("Quel fichier voulez vous supprimer? ");
		System.out.print("Nom du fichier enregistré: ");
		String nf = sc.nextLine();

		try {
			HdfsClient.HdfsDelete(nf);
		} catch (ErreurEnvoiInformationsServeurException e) {
			System.out.println("Une erreur a été levé, veuillez réessayer (nom du fichier pas bon?) ");
		}
	}

	/**
	 * Afficher les datanodes sur lesquelles un fichier est enregistré.
	 */
	private static void hdfs4() {
		System.out.println("Afficher les datanodes sur lesquelles un fichier est enregistré :");
		System.out.print("Nom du fichier enregistré: ");
		String nf = sc.nextLine();
		ListesServeurs l = null;
		try {
			l = HdfsClient.HdfsGetDataNodesOfFile(nf);
		} catch (ErreurEnvoiInformationsServeurException e) {
			System.out.println("Une erreur a été levé, veuillez réessayer (nom du fichier pas bon?) ");
		}

		if (l != null) {
			int i = 0;
			for (String machine : l.getMain()) {
				System.out.println("Fragment " + i++ + " enregistré sur " + machine);
			}
		} else {
			System.out.println("Le fichier n'est pas enregistré");
		}
	}
	
	/**
	 * Afficher tous les fichiers enregistrés.
	 */
	private static void hdfs5() {
		System.out.println("Afficher les fichiers enregistrés :");
		HdfsClient.HdfsCarte();
	}

	/**
	 * Lancer un map reduce sur un fichier.
	 */
	private static void hidoop(boolean avecReducerMultiple) {
		System.out.println("Lancement d'une application map reduce sur un fichier");
		System.out.print("Nom du fichier : ");
		String nf = sc.nextLine();
		System.out.println("Quelle appli? \n 1. WordCount \n 2. Calcul de Pi \n 3. Algorithme de Knuth \n 4. Page ranking \n 5. Calcul de l'indice de coincidence \n 6. Calcul des fréquences des lettres");
		System.out.print("Choix : ");
		int i = sc.nextInt();
		
		JobInterfaceX job = null;
		if(avecReducerMultiple)
			job = new JobX();
		else
			job = new Job();

		job.setInputFormat(Format.Type.KV);
		job.setInputFname(nf);
		job.setOutputFname(nf + "-resultat_final");
		job.setOutputFormat(Format.Type.KV);

		MapReduce mr = null;
		switch(i){
		case 1:
		default:
			mr = new MyMapReduce();
			break;
		case 2:
			mr = new MapReducePi();
			break;
		case 3:
			mr = new MapReduceKnuth();
			break;
		case 4:
			// PAGE RANKING A AJOUTER
			mr = new MyMapReduce();
			break;
		case 5:
			mr = new MapReduceFreqLetter();
			break;
		case 6:
			mr = new MapReduceIndiceCoincidence();
			break;
		}
		
		long t1 = System.currentTimeMillis();
		job.startJob(mr);
		long t2 = System.currentTimeMillis();

		System.out.println("time in ms =" + (t2 - t1));
	}
	
}
