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
 * Calculer l'indice de coincidence du fichier
 *
 */
public class MapReduceIndiceCoincidence implements MapReduce {

	private static final long serialVersionUID = 1L;

	@Override
	public void map(FormatReader reader, FormatWriter writer) {

		/* Initialisation des variables. */
		Map<String, Float> myMap = new HashMap<>();
		KV kv;

		/* Lire. */
		while ((kv = reader.read()) != null) {
			/* Récupérer un String correspondant à la ligne. */
			String ligne = kv.v;
			float indice = calculIndiceCoincidence(ligne);

			myMap.put(ligne, indice);

		}

		for (String ligne : myMap.keySet()) {
			if (myMap.get(ligne) != 0) {
				KV kv_write = new KV(ligne, myMap.get(ligne).toString());
				writer.write(kv_write);
			}
		}
	}
	
	@Override
	public void reduce(FormatReader reader, FormatWriter writer) {
		KV kv;
		float resultat = 0;
		float i = 0;
		while ((kv = reader.read()) != null) {
			resultat += Float.parseFloat(kv.v);
			i++;
		}
		resultat = resultat / i;
		
		writer.write(new KV("Moyenne de l'indice de coincidence:", String.valueOf(resultat) ));
	}

	private static float calculIndiceCoincidence(String texte) {

		texte = texte.replaceAll(" ", "").toLowerCase();
		texte = texte.replaceAll("é", "e").toLowerCase();
		texte = texte.replaceAll("è", "e").toLowerCase();
		texte = texte.replaceAll("à", "a").toLowerCase();
		texte = texte.replaceAll("[^A-Za-z0-9]", "");

		char[] alphabet = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
				's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };
		int nbLettreMessage = texte.length();

		if (nbLettreMessage == 0) {
			return 0;
		}
		float indice = 0;
		/* Itérer sur chaque lettre de l'alphabet */
		for (char lettreAlphabet : alphabet) {
			int n = 0;
			for (char lettreMsg : texte.toCharArray()) {
				if (lettreMsg == lettreAlphabet) {
					n++;
				}
			}
			float num = n * (n - 1);
			float den = nbLettreMessage * (nbLettreMessage - 1);
			if (den == 0) {
				return 0;
			}
			indice = indice + (num / den);
		}

		return indice;
	}

	public static void main(String args[]) {

		Job j = new Job();
		j.setInputFormat(Format.Type.LINE);
		j.setInputFname(args[0]);
		long t1 = System.currentTimeMillis();
		j.startJob(new MapReduceIndiceCoincidence());
		long t2 = System.currentTimeMillis();
		System.out.println("time in ms =" + (t2 - t1));
		System.exit(0);
	}

}
