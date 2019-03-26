package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface CallbackInterface extends Remote {
	
	// Bloque l'application Job grâce à un sémaphore
	void bloquer() throws RemoteException, InterruptedException;

	// Release un sémaphore, 
	void finTache() throws RemoteException;

}
