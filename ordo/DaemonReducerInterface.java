package ordo;

import java.rmi.Remote;
import java.util.Map;

import formats.Format.Type;
import map.MapReduce;

public interface DaemonReducerInterface extends Remote{
	void runMapsAndReduce(MapReduce m, Map<Integer, String> serveurs, String inputFname, String outputFname, Type inputFormat, Type outputFormat,
			Callback callbackJob);
}