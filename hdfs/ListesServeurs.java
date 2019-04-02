package hdfs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ListesServeurs implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8768526012583861445L;
	private List<String> main;
	private List<String> backup;
	private boolean hasBackup;
	
	public ListesServeurs(boolean withSave){
		this.setMain(new ArrayList<String>());
		if(withSave) this.setBackup(new ArrayList<String>());
		else this.setBackup(null);
		this.hasBackup = withSave;
	}

	public List<String> getMain() {
		return main;
	}

	public void setMain(List<String> main) {
		this.main = main;
	}

	public List<String> getBackup() {
		return backup;
	}

	public void setBackup(List<String> backup) {
		this.backup = backup;
	}
	
	// ON ENREGISTRE LE FRAGMENT AU i + 1 serveur
	public String getSavedFragment(int i) {
		if(this.backup != null)
			return this.backup.get((i + 1)%this.backup.size());
		else
			return null;
	}
	
	public String toString(){
		String res = "Main : " + this.main.toString();
		if(this.backup != null) res += " / Backup : " + this.backup.toString();
		return res;
	}

	public boolean hasBackup() {
		return hasBackup;
	}

	public void setHasBackup(boolean hasBackup) {
		this.hasBackup = hasBackup;
	}
	
}
