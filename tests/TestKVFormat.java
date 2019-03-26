package tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

import exceptions.ErreurLectureFichierException;
import exceptions.ErreurOuvertureFichierException;

import static org.junit.Assert.assertEquals;

import formats.Format.OpenMode;
import formats.KV;
import formats.KVFormat;

/**
 * Classe de test pour la classe KVFormat.
 * 
 */
public class TestKVFormat {

	/* Nom du fichier pour le test */
	public static final String FILE_PATH = "data/";
	public static final String FILE_NAME_READ = "test_kv_format_read.txt";
	public static final String FILE_NAME_WRITE = "test_kv_format_write.txt";

	/** Fichier pour les tests. */
	private static FileWriter fichier;

	/** Definition de different kv pour les tests. */
	private static KV kv1, kv2, kv3;

	/**
	 * Methode pour initialiser notre test.
	 */
	@Before
	public void setUp() {

		/* Création des KV. */
		kv1 = new KV("GLS", "18");
		kv2 = new KV("PF", "8");
		kv3 = new KV("Opti", "10");
	}

	/**
	 * Test de la fonction read.
	 */
	@Test
	public void testReadOptimale() {

		ArrayList<KV> KV_test_read = setListKVRead();

		setUpFichier(KV_test_read, FILE_NAME_READ);

		/* Creation d'une liste pour stocker les KV lus. */
		ArrayList<KV> kvLues = new ArrayList<KV>();

		KVFormat kvFormat = new KVFormat();
		kvFormat.setFname(FILE_NAME_READ);
		kvFormat.setPath(FILE_PATH);
		kvFormat.open(OpenMode.R);

		/* Lire le fichier avec la méthode de KVFromat. */
		KV kv;
		while ((kv = kvFormat.read()) != null) {
			kvLues.add(kv);
		}

		/* Test de la taille. */
		assertEquals(kvLues.size(), KV_test_read.size());

		/* Test de l'égalité des fichier. */
		for (int i = 0; i < kvLues.size(); i++) {
			// System.out.println(kvLues.get(i));
			assertEquals(kvLues.get(i).k, KV_test_read.get(i).k);
			assertEquals(kvLues.get(i).v, KV_test_read.get(i).v);
		}

		/* Supprimer le fichier. */
		File file = new File(FILE_PATH + FILE_NAME_READ);
		file.delete();
		kvFormat.close();

	}

	/**
	 * Test de la fonction Write.
	 */
	@Test
	public void testWriteOptimale() {

		ArrayList<KV> KV_test_write = setListKVRead();

		setUpFichier(KV_test_write, FILE_NAME_WRITE);

		KVFormat kvFormat = new KVFormat();
		kvFormat.setFname(FILE_NAME_WRITE);
		kvFormat.setPath(FILE_PATH);
		kvFormat.open(OpenMode.W);

		/* Ecriture avec la méthode de KV Format. */
		KV monKv = new KV("Prog_conc", "20");
		KV_test_write.add(monKv);
		kvFormat.write(monKv);

		try {
			/*
			 * Vérifier que le texte n'a pas bougé et que le nouveau KV a bien
			 * été ajouté.
			 */
			int i = 0;
			String ligne;
			BufferedReader br = new BufferedReader(new FileReader(FILE_PATH + FILE_NAME_WRITE));

			while ((ligne = br.readLine()) != null) {
				assertEquals(ligne, KV_test_write.get(i).k + "<->" + KV_test_write.get(i).v);
				i++;

			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		/* Supprimer le fichier. */
		File file = new File(FILE_PATH + FILE_NAME_WRITE);
		file.delete();
		kvFormat.close();

	}

	/**
	 * Class de test pour la levée d'exception de read.
	 */
	@Test(expected = ErreurLectureFichierException.class)
	public void TestReadException() {
		KVFormat kvf = new KVFormat();
		kvf.read();
	}

	/**
	 * Class de test pour la levée d'exception de read.
	 */
	@Test(expected = ErreurLectureFichierException.class)
	public void TestReadExceptionFichierInexistant() {
		KVFormat kvf = new KVFormat();
		kvf.setFname("wrvongpath");
		kvf.read();
	}

	/**
	 * Class de test pour la levée d'exception de open.
	 */
	@Test(expected = ErreurOuvertureFichierException.class)
	public void TestExceptionOpen() {
		KVFormat kvf = new KVFormat();
		kvf.setFname("wvrongpath");
		kvf.open(OpenMode.R);
	}

	/**
	 * Class de test pour getLength
	 */
	@Test
	public void TestGetLength() {
		ArrayList<KV> KV_test_write = setListKVRead();

		setUpFichier(KV_test_write, FILE_NAME_READ);

		KVFormat kvFormat = new KVFormat();

		kvFormat.setFname(FILE_NAME_READ);
		kvFormat.setPath(FILE_PATH);

		long length = kvFormat.getLength();

		/* Supprimer le fichier. */
		File file = new File(FILE_PATH + FILE_NAME_READ);
		file.delete();

		assertEquals(length, 3);

	}

	private ArrayList<KV> setListKVRead() {
		ArrayList<KV> liste = new ArrayList<KV>();
		liste.add(kv1);
		liste.add(kv2);
		liste.add(kv3);
		return liste;
	}

	private FileWriter setUpFichier(ArrayList<KV> liste, String fileName) {
		try {
			fichier = new FileWriter(FILE_PATH + fileName, true);
			for (KV kv : liste) {
				fichier.write(kv.k + "<->" + kv.v + "\n");
			}
			fichier.close();
			return fichier;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public void tearDown() {
		/* Supprimer le fichier. */
		File file = new File(FILE_PATH + FILE_NAME_READ);
		file.delete();
		File filer = new File(FILE_PATH + FILE_NAME_WRITE);
		filer.delete();
	}

}