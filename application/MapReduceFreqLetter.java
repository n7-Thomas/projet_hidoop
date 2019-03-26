package application;

import java.util.HashMap;
import java.util.Map;
import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;
import ordo.Job;

/**
 * Ici on va calculer la freq d'apparition des lettres.
 * 
 * @author fngbala
 *
 */
public class MapReduceFreqLetter implements MapReduce {

	private static final long serialVersionUID = 1L;

	@Override
	public void map(FormatReader reader, FormatWriter writer) {

		/* Initialisation des variables. */
		Map<Character, Integer> myMap = new HashMap<>();
		Map<Character, Float> myMapFloat = new HashMap<>();

		KV kv;
		int nbLettreTotal = 0;

		/* Lire. */
		while ((kv = reader.read()) != null) {
			/* Récupérer un String correspondant à la ligne. */
			String ligne = kv.v;

			/* Supprimer les esapces et tout passer en minuscule. */
			ligne = ligne.replaceAll(" ", "");
			ligne = ligne.replaceAll("é", "e");
			ligne = ligne.replaceAll("è", "e");
			ligne = ligne.replaceAll("à", "a");
			ligne = ligne.replaceAll("[^A-Za-z0-9]", "");

			/* Itérer sur chaque lettre de la ligne. */
			for (char lettre : ligne.toCharArray()) {

				/* Vérifier si la lettre est presente dans la map. */
				if (myMapFloat.containsKey(lettre)) {
					myMapFloat.put(lettre, myMapFloat.get(lettre) + (float) 1);
				} else {
					myMapFloat.put(lettre, (float) 1);
				}
				/* Incrémenter le nb de lettre total. */
				nbLettreTotal++;
			}

		}
		/* Calculer la freq d'apparition et écrire la lettre dans un fichier. */
		for (char lettre : myMapFloat.keySet()) {
			int pourcentage = Math.round((myMapFloat.get(lettre) / nbLettreTotal) * 100);
			if (pourcentage != 0) {
				myMap.put(lettre, pourcentage);
				//System.out.println(pourcentage);
			}
		}

		for (char lettre : myMap.keySet()) {
			KV kv_write = new KV(Character.toString(lettre), myMap.get(lettre).toString());
			writer.write(kv_write);
		}

	}

	@Override
	public void reduce(FormatReader reader, FormatWriter writer) {
		Map<String, Integer> hm = new HashMap<>();
		KV kv;
		while ((kv = reader.read()) != null) {
			if (hm.containsKey(kv.k))
				hm.put(kv.k, hm.get(kv.k) + Integer.parseInt(kv.v));
			else
				hm.put(kv.k, Integer.parseInt(kv.v));
		}
		for (String k : hm.keySet())
			writer.write(new KV(k, hm.get(k).toString()));
	}

	public static void main(String args[]) {
		Job j = new Job();
		j.setInputFormat(Format.Type.LINE);
		j.setInputFname(args[0]);
		long t1 = System.currentTimeMillis();
		j.startJob(new MapReduceFreqLetter());
		long t2 = System.currentTimeMillis();
		System.out.println("time in ms =" + (t2 - t1));
		System.exit(0);
		System.out.println("time in ms =" + (t2 - t1));

	}

}
