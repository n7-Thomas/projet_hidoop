package tests;

import static org.junit.Assert.assertEquals;

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
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;
import formats.Format.OpenMode;

public class TestLineFormat {

	/* Nom du fichier pour le test */
	public static final String FILE_PATH = "data/";
	public static final String FILE_NAME_READ = "test_line_format_read.txt";
	public static final String FILE_NAME_WRITE = "test_line_format_write.txt";

	/** Fichier pour les tests. */
	private static FileWriter fichier;

	/** Definition de different kv pour les tests. */
	private static String l1, l2, l3;

	/**
	 * Methode pour initialiser notre test.
	 */
	@Before
	public void setUp() {

		/* Création des KV. */
		l1 = "Thomas Darget";
		l2 = "Florian N'gbala";
		l3 = "Léane Porcheray";
	}

	/**
	 * Test de la fonction read.
	 */
	@Test
	public void testReadOptimal() {

		ArrayList<String> test_read = setListRead();

		setUpFichier(test_read, FILE_NAME_READ);

		/* Creation d'une liste pour stocker les KV lus. */
		ArrayList<KV> kvLues = new ArrayList<KV>();

		LineFormat lineFormat = new LineFormat();
		lineFormat.setFname(FILE_NAME_READ);
		lineFormat.setPath(FILE_PATH);
		lineFormat.open(OpenMode.R);

		/* Lire le fichier avec la méthode de KVFromat. */
		KV kv;
		while ((kv = lineFormat.read()) != null) {
			kvLues.add(kv);
		}

		/* Test de la taille. */
		assertEquals(kvLues.size(), test_read.size());

		/* Test de l'égalité des fichier. */
		for (int i = 0; i < kvLues.size(); i++) {
			assertEquals(kvLues.get(i).k, Integer.toString(i + 1));
			assertEquals(kvLues.get(i).v, test_read.get(i));
		}

		/* Supprimer le fichier. */
		lineFormat.close();
		File file = new File(FILE_PATH + FILE_NAME_READ);
		file.delete();
	}

	/**
	 * Test de la fonction Write.
	 */
	@Test
	public void testWriteOptimal() {

		ArrayList<String> test_write = setListRead();

		setUpFichier(test_write, FILE_NAME_WRITE);

		LineFormat lformat = new LineFormat();
		lformat.setFname(FILE_NAME_WRITE);
		lformat.setPath("data/");
		lformat.open(OpenMode.W);

		/* Ecriture. */
		String l = "Quentin Gillet";
		test_write.add(l);
		lformat.write(new KV(Integer.toString(test_write.size() + 1), l));

		try {
			/*
			 * Vérifier que le texte n'a pas bougé et que le nouveau KV a bien
			 * été ajouté.
			 */
			int i = 0;
			String ligne;
			BufferedReader br = new BufferedReader(new FileReader(FILE_PATH + FILE_NAME_WRITE));

			while ((ligne = br.readLine()) != null) {
				assertEquals(Integer.toString(i + 1) + "<->" + ligne,
						Integer.toString(i + 1) + "<->" + test_write.get(i));
				i++;
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		/* Supprimer le fichier. */
		File file = new File(FILE_PATH + FILE_NAME_WRITE);
		file.delete();
		lformat.close();
	}

	/**
	 * Class de test pour la levée d'exception de read.
	 */
	@Test(expected = ErreurLectureFichierException.class)
	public void TestReadException() {
		LineFormat lf = new LineFormat();
		lf.read();
	}

	/**
	 * Class de test pour la levée d'exception de read.
	 */
	@Test(expected = ErreurLectureFichierException.class)
	public void TestReadExceptionFichierInexistant() {
		LineFormat lf = new LineFormat();
		lf.setFname("wrongpath");
		lf.read();
	}

	/**
	 * Class de test pour la levée d'exception de open.
	 */
	@Test(expected = ErreurOuvertureFichierException.class)
	public void TestExceptionOpen() {
		KVFormat kvf = new KVFormat();
		kvf.setFname("wrongpath");
		kvf.open(OpenMode.R);
	}

	/**
	 * Class de test pour getLength
	 */
	@Test
	public void TestGetLength() {

		ArrayList<String> KV_test_write = setListRead();

		setUpFichier(KV_test_write, FILE_NAME_READ);

		LineFormat lineformat = new LineFormat();
		lineformat.setFname(FILE_NAME_READ);
		lineformat.setPath(FILE_PATH);
		long length = lineformat.getLength();
		/* Supprimer le fichier. */

		lineformat.close();
		File file = new File(FILE_PATH + FILE_NAME_READ);
		file.delete();

		assertEquals(length, 3);
	}

	private ArrayList<String> setListRead() {
		ArrayList<String> liste = new ArrayList<String>();
		liste.add(l1);
		liste.add(l2);
		liste.add(l3);
		return liste;
	}

	private FileWriter setUpFichier(ArrayList<String> liste, String fileName) {
		try {
			fichier = new FileWriter(FILE_PATH + fileName, true);
			for (String s : liste) {
				fichier.write(s + "\n");
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
		File file = new File("data/test_line_format_read.txt");
		file.delete();
		File filer = new File(FILE_PATH + FILE_NAME_WRITE);
		filer.delete();
	}

}