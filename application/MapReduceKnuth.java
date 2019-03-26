package application;

import java.util.ArrayList;

import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;
import ordo.Job;

public class MapReduceKnuth implements MapReduce {

	/**
	 * Algorithme de Knuth.
	 */
	private static final long serialVersionUID = 1L;

	// Tableau des correspondances
	private ArrayList<Integer> t = new ArrayList<Integer>();

	private void tableau(String p) {
		int i = 0;
		int j = -1;
		char c = '\0';
		t.set(0, j);
		while (i < p.length()) {

			if (p.charAt(i) == c) {
				t.set(i + 1, j + 1);
				j++;
				i++;
			} else if (j > 0) {
				j = t.get(j);
			} else {
				t.set(i + 1, 0);
				i++;
				j = 0;
			}
			c = p.charAt(j);
		}
	}

	private int recherche(String p, String s) {
		int m = 0;
		int i = 0;
		while (m + i < s.length() && i < p.length()) {
			if (s.charAt(m + i) == p.charAt(i)) {
				i++;
			} else {
				m += i - this.t.get(i);

				if (i > 0) {
					i = this.t.get(i);

				}
			}
		}

		if (i >= p.length()) {
			return m;
		} else {
			return -1;
		}
	}

	@Override
	public void map(FormatReader reader, FormatWriter writer) {
		KV kv;
		while ((kv = reader.read()) != null) {
			String texte = kv.v; // Le texte
			String motif = kv.k; // Le "motif" à rechercher
			this.tableau(motif);
			writer.write(new KV(motif, Integer.toString(this.recherche(motif, texte))));
		}
	}

	@Override
	public void reduce(FormatReader reader, FormatWriter writer) {
		KV kv;
		while ((kv = reader.read()) != null) {
			writer.write(new KV(kv.k, kv.v));
		}

	}

	public static void main(String args[]) {
		Job j = new Job();
		j.setInputFormat(Format.Type.LINE);
		j.setInputFname(args[0]);
		long t1 = System.currentTimeMillis();
		j.startJob(new MapReducePi());
		long t2 = System.currentTimeMillis();
		System.out.println("time in ms =" + (t2 - t1));
		System.exit(0);
	}

}
