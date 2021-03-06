package config;

import java.io.File;

public class Project {
	
	// Affichage de certains logs.
	public static final boolean DEBUG = false;
		

	
	// Path des fichiers de base
	//public static final String PATH_FILES = "data/";
	public static final String PATH_FILES = "nosave/";
	
	
	// Path des fichiers enregistrés sur les datanodes
	public static final String PATHfichiersSurDisque = "/work/";
	
	// Path du projet sans le /src/
	private static String pathA = (new File(".")).getAbsolutePath();
	public static final String PATHB= pathA.substring(0, pathA.length() - 5); // Pour enlever le /src/.
	public static final String PATH = "/home/tdarget/";
	
	
	// Informations du NameNode
	public static final String AdresseNameNode = "magicarpe";
	public static final int PortNameNode = 8081;
	
	// Informations des DataNodes
	public static final String[] nomMachinesDataNodes = {"zztop","portreal", "tully", "arryn", "dorne", "carpe", "salameche", "rattata", "roucool", "pikachu" };
	//public static final String[] nomMachinesDataNodes = {"localhost", "localhost"};
	public static final int PortDataNode = 8082;
	
	// Informations des Daemons
	//public static final String[] AdresseDaemon = nomMachinesDataNodes;
	public static final int PortDaemon = 8083;
	public static final int NombreOuvriersParDaemons = 10;
	

	// Informations du Callback
	public static final int PortCallback = 8084;
	public static final String AdresseCallback = "magicarpe";


	// Nombre maximum de KVMax enregistré dans un fichier.
	public static final int NombreKVMax = 4000000;
	
	// Nombre de serveurs qui sont utilisés parmis la liste plus haut
	public static final int NombreServeursUtiles = 10;


	
	public static final String[] nomMachineDaemonReducer = {"taupiqueur", "melofee"};
	public static final int PortDaemonReducer = 8085;

	
}
