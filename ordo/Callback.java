package ordo;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Callback extends UnicastRemoteObject implements Serializable, CallbackInterface {
	private static final long serialVersionUID = 4255026383578684902L;

	// Sémaphore d'attente au retour 
	private Semaphore sem;
	
	// Nombre de taches à bloquer
	private int nombreTaches;
	
	// Exclusion mutuelle de l'accès à la sémaphore
	private Lock l;

	/**
	 * Outil de callback pour attendre la fin des tâches 
	 * @param nbTaches
	 * @throws RemoteException
	 */
	public Callback(int nbTaches) throws RemoteException {
		super();
		this.sem = new Semaphore(0);
		this.nombreTaches = nbTaches;
		this.l = new ReentrantLock();
	}

	/**
	 * Reception d'une fin de map
	 */
	public void finTache() throws RemoteException {
		l.lock();
		try {
			System.out.println("Reception d'une fin de tâche");
			this.sem.release();
		} finally {
			l.unlock();
		}
	}

	/**
	 * Bloquer tant qu'on a pas reçu toutes les fins de taches
	 */
	public void bloquer() throws InterruptedException, RemoteException {
		System.out.println("Bloquage du callback");
		for (int i = 1; i <= nombreTaches; i++) {
			//l.lock();
			try {
				this.sem.acquire();
			} finally {
				//l.unlock();
				System.out.println("Déblocage n°" + i);
			}
		}
	}

}
