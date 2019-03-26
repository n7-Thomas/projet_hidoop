package hdfs;

import java.io.Serializable;

public class Requete implements Serializable {
	private static final long serialVersionUID = 1L;

	public enum TypeRequete {
		Recuperer, Ecrire, Supprimer, RecupererIntermediaires, SupprimerIntermediaires;
	}

	private TypeRequete type;
	private String nomFichier;
	private int identifiantFichier;
	private int nombreKVtransfert;

	public Requete(TypeRequete t, String nomFichier, int id, int nb) {
		this.setIdentifiantFichier(id);
		this.setNomFichier(nomFichier);
		this.setType(t);
		this.setNombreKVtransfert(nb);
	}

	public int getIdentifiantFichier() {
		return identifiantFichier;
	}

	public void setIdentifiantFichier(int identifiantFichier) {
		this.identifiantFichier = identifiantFichier;
	}

	public String getNomFichier() {
		return nomFichier;
	}

	public void setNomFichier(String nomFichier) {
		this.nomFichier = nomFichier;
	}

	public TypeRequete getType() {
		return type;
	}

	public void setType(TypeRequete type) {
		this.type = type;
	}

	public int getNombreKVtransfert() {
		return nombreKVtransfert;
	}

	public void setNombreKVtransfert(int nombreKVtransfert) {
		this.nombreKVtransfert = nombreKVtransfert;
	}

}
