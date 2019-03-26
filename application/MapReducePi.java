package application;

import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;
import ordo.Job;

public class MapReducePi implements MapReduce {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	// MapReduce that computes pi
	@Override
	public void map(FormatReader reader, FormatWriter writer) {
		KV kv;
		while ((kv = reader.read()) != null) {
			String nbPointsString = kv.k;	// On récupère le nombre de points
			Integer nbPoints = Integer.parseInt(nbPointsString);
			int nbPointsDansCercle = 0;
			
			// on place les points au hasard dans un carré de coté 1
			for (int i=0; i<nbPoints; i++) {
				double x = Math.random();
				double y = Math.random();
				if(Math.pow(x, 2) + Math.pow(y, 2) < 1) {
					// il est dans le cercle
					nbPointsDansCercle ++;
				}
			}
			
			double pi = 4*nbPointsDansCercle/nbPoints;
			writer.write(new KV(nbPoints.toString(),Double.toString(pi)));
			
		}
	}

	
	@Override
	public void reduce(FormatReader reader, FormatWriter writer) {
		KV kv;
		Integer nombreResultats = 0;
		double sommePi = 0;
		while ((kv = reader.read()) != null) {
			sommePi += Double.parseDouble(kv.v);
			nombreResultats++;
		}
		
		double moyenne = sommePi/nombreResultats;
		
		writer.write(new KV(nombreResultats.toString(),Double.toString(moyenne)));
		
	}
	
	public static void main(String args[]) {

		Job j = new Job();
		j.setInputFormat(Format.Type.LINE);
		j.setInputFname("a.txt");
		long t1 = System.currentTimeMillis();
		j.startJob(new MapReducePi());
		long t2 = System.currentTimeMillis();
		System.out.println("time in ms =" + (t2 - t1));
		System.exit(0);
	}

}
