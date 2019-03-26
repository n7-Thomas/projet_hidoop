package formats;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import formats.KV;

import config.Project;
import exceptions.ErreurEcritureFichierException;
import exceptions.ErreurLectureFichierException;
import exceptions.ErreurOuvertureFichierException;

public class KVFormat implements Format {

	private static final long serialVersionUID = 1L;

	/**
	 * Nom du fichier
	 */
	private String fname;

	/**
	 * Fichier 
	 */
	private File fichier;
	
	/**
	 * Stream du fichier en lecture
	 */
	private FileReader fr;
	
	/**
	 * Buffer du fichier en lecture
	 */
	private BufferedReader br;

	/**
	 * Stream du fichier en écriture
	 */
	private FileWriter fw;

	/**
	 * Index propre au fichier
	 */
	private int index;
	
	/**
	 * Path propre
	 */
	private String path = Project.PATHfichiersSurDisque;
	
	
	/**
	 * Lire un KV dans un fichier LineFormat.
	 * 
	 * @return KV
	 * @throws ErreurLectureFichierException
	 */
	@Override
	public KV read() throws ErreurLectureFichierException {

		/* Gestion d'erreur */
		if (br == null) {
			System.out.println("[ERREUR] - KVFormat.java - BufferedReader pas initialisé ");
			throw new ErreurLectureFichierException("BufferedReader pas initialisé");
		}

		if (fr == null) {
			System.out.println("[ERREUR] - KVFormat.java - FileReader pas initialisé ");
			throw new ErreurLectureFichierException("FileReader pas initialisé");
		} 
		
		try {
			/* Lire la ligne. */
			String ligne = br.readLine();
			
			/* Si la ligne n'est pas nulle construire un KV. */
			if (ligne == ""){
				return new KV(String.valueOf(index), "");
			}else if (ligne != null) {
				String[] tabLigne = ligne.split("<->");	
				if(tabLigne.length != 2)
					return new KV(String.valueOf(index), "");
				else				
					return new KV(tabLigne[0], tabLigne[1]);

				/* Sinon on renvoie null */
			} else {
				return null;
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new ErreurLectureFichierException("Erreur lecture du fichier");
		}
	}

	/**
	 * Ecrire un KV sur le fichier.
	 * 
	 * @param record
	 * @throws ErreurEcritureFichierException
	 */
	@Override
	public void write(KV record) throws ErreurEcritureFichierException {

		/* Gestion d'erreur */
		if (this.fw == null) {
			throw new ErreurEcritureFichierException("[ERREUR] - KVFormat.java - FileWriter pas initialisé ");
		}


		/* Ecriture d'un KV dans un fichier */
		try {
			// Ecriture sur le fichier
			fw.write(record.k + "<->" + record.v + "\n");

		} catch (Exception e) {
			e.printStackTrace();
			throw new ErreurEcritureFichierException("[ERREUR] - KVFormat.java - File pas initialisé : " + this.fname);
		}

	}

	/**
	 * Ouvrir un fichier en écriture ou lecture. Renvoie une exception si
	 * erreur.
	 * 
	 * @param mode
	 * @throws ErreurOuvertureFichierException
	 */
	@Override
	public void open(OpenMode mode) throws ErreurOuvertureFichierException {

		/* Gestion des erreurs */
		if (this.fname == null) {
			System.out.println("[ERREUR] - LineFormat.java - Ouverture du fichier: pas de fichier déclaré ");
		} else {

			/* Initialisation du fichier */
			this.index = 0;
			
			// Création du fichier 
			String url = this.path + this.fname;
			this.fichier = new File(url);
			
			/* En mode lecture */
			if (mode == OpenMode.R) {
				
				// Création d'un FileReader et d'un Buffered Reader
				try {
					this.fr = new FileReader(this.fichier);
					this.br = new BufferedReader(this.fr);
				} catch (FileNotFoundException e) {
					System.out.println("[ERREUR] - KVFormat.java - Ouverture du fichier: fichier " + url + " non trouvé");
				}

				// Gestion d'erreur
				if (this.br == null || this.fr == null)
					throw new ErreurOuvertureFichierException("[ERREUR] - KVFormat.java - Ouverture du fichier  " + url + " en lecture");
			
			/* En mode écriture */
			} else {
			
				// Création d'un FileWriter
				try {
					this.fw = new FileWriter(fichier, true);
				} catch (Exception e) {
					System.out.println("[ERREUR] - KVFormat.java - Ouverture du fichier: fichier  " + url + " non trouvé");
				}

				// Gestion d'erreur
				if (this.fw == null)
					throw new ErreurOuvertureFichierException("[ERREUR] - KVFormat.java - Ouverture du fichier  " + url + " en écriture");
			}
			
		}
	}

	/**
	 * Fermer le fichier et les streams pour le lire.
	 */
	@Override
	public void close() {

		// Index mis à zéro
		this.index = 0;
		
		
		// Fermeture des streams
		if (this.fw != null) {
			try {
				fw.close();
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("[ERREUR] - KVFormat.java - Fermeture du filewriter");
			}
		}

		if (this.br != null) {
			try {
				br.close();
			} catch (IOException e) {
				System.out.println("[ERREUR] - KVFormat.java - Fermeture du buffered reader");
			}
		}

		if (this.fr != null) {
			try {
				fr.close();
			} catch (IOException e) {
				System.out.println("[ERREUR] - KVFormat.java - Fermeture du file reader");
			}
		}
	}

	/**
	 * Renvoyer l'index actuel du fichier.
	 * 
	 * @return this.index
	 */
	@Override
	public long getIndex() {
		return this.index;
	}

	/**
	 * Renvoyer le nom du fichier.
	 * 
	 * @return this.fname
	 */
	@Override
	public String getFname() {
		return this.fname;
	}

	/**
	 * Modifier le nom du fichier.
	 * 
	 * @param fname
	 */
	@Override
	public void setFname(String fname) {
		this.fname = fname;
	}
	
	/**
	 * Renvoyer la longueur du fichier.
	 */
	@Override
	public long getLength() {
		
		if(this.fname == null)
			return 0;

		long taille = 0;
		this.open(OpenMode.R);
		
		while(this.read() != null )
			taille++;
		
		this.close();
		
		
		return taille;
	}

	/**
	 * Changer le path
	 * @param path
	 */
	public void setPath(String path) {
		this.path = path;
	}

}