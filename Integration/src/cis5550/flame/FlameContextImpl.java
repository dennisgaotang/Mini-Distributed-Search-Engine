package cis5550.flame;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Vector;
import java.net.URLEncoder;

import cis5550.generic.Worker;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.*;
import cis5550.tools.Partitioner.Partition;

public class FlameContextImpl implements FlameContext, Serializable {
	private static final Logger logger
			= Logger.getLogger(FlameContextImpl.class);
	final String DEFAULT_MESSAGE_FOR_SUBMIT = "output() has never been invokes, no output for /submit";
	String finalOutput;
	public static int nextRDDSequenceNum = 1;
//	KVSClient kvs;
	String coordinatorJarName;
	String coordinatorAddress;
	private int keyRangesPerWorker = 1;

	public FlameContextImpl(String jarName, String coordinatorAddress) {
		finalOutput = null; 
		this.coordinatorAddress = coordinatorAddress;
		coordinatorJarName = jarName;
	}

	@Override
	public KVSClient getKVS() {
		return new KVSClient(coordinatorAddress);
	}

	@Override
	public void output(String s) {
		// instantiate a new String
		if (finalOutput == null) {
			finalOutput = "";
		}
		finalOutput += s;
	}

	@Override
	public FlameRDD parallelize(List<String> list) throws Exception {
		// generate a name for the RDD
		String RDDTableName = generateTableName(nextRDDSequenceNum, "RDD");
		nextRDDSequenceNum++;
		// using kvs client to create a RDD table
		KVSClient kvs = getKVS();
		int seed = 1;
		for (String s: list) {
			Row row = new Row(generateUniqueRandomRowKey(seed));
			seed++;
			row.put("value", s);
			kvs.putRow(RDDTableName, row);
		}
		FlameRDD flameRDD = new FlameRDDImpl(this, kvs, RDDTableName);
		return flameRDD;
	}

	


	private String generateUniqueRandomRowKey(int seed) {
		return Hasher.hash(Integer.toString(seed));
	}

	// added method(not in interface) to get the output() concatenated string
	public String getFinalOutput() {
		if (finalOutput == null) return DEFAULT_MESSAGE_FOR_SUBMIT;
		return finalOutput;
	}
	
	// method invoked by FlameRDDImpl or FlamePairRDDImpl in manipulating method, return the output RDD or RDDPair Name
	public String invokeOperation(Object rddOrPairRDD, String operationName, byte[] lambda ) throws Exception {
		// 1. Generate fresh output table name
		String inputRDDOrPairRDDName = null;
		if (rddOrPairRDD instanceof FlameRDDImpl) inputRDDOrPairRDDName = ((FlameRDDImpl)rddOrPairRDD).kvsTableName;
		if (rddOrPairRDD instanceof FlamePairRDDImpl) inputRDDOrPairRDDName = ((FlamePairRDDImpl)rddOrPairRDD).kvsTableName;
		String[] splitParts = operationName.split("/"); // {"rdd", "flapMap"}
		String optionalTableNameForIntersect = operationName.equals("/rdd/intersection")? ("_" + inputRDDOrPairRDDName + "_intersects_" + lambda.toString()) : "";
		String optionalTableNameForSample = operationName.equals("/rdd/sample")? ("_" + Double.parseDouble(new String(lambda))):"";
		String outputTableType = getOutputTableType(operationName);
		String outputTableName = generateTableName(nextRDDSequenceNum, outputTableType) + "_" + splitParts[splitParts.length-1] + optionalTableNameForIntersect + optionalTableNameForSample;
		nextRDDSequenceNum++;

		// 2. Partition
		// kvs already sorted out the workers according to id
		Partitioner p = new Partitioner();
		p.setKeyRangesPerWorker(this.keyRangesPerWorker);
		KVSClient kvs = getKVS();


		int numKVSWorkers = kvs.numWorkers();
		for (int i = 0; i < (numKVSWorkers-1); i++) {
			String kvsWorkerAddrPort = kvs.getWorkerAddress(i);
			String curKVSWorkerID = kvs.getWorkerID(i);
			String nextKVSWorkerID = kvs.getWorkerID(i+1);
			p.addKVSWorker(kvsWorkerAddrPort, curKVSWorkerID, nextKVSWorkerID);
		}
		// for the last kvsWorker
		String lastKVSWorkerAddrPort = kvs.getWorkerAddress(numKVSWorkers - 1);
		String lastKVSWorkerID = kvs.getWorkerID(numKVSWorkers - 1);
		String firstKVSWorkerID = kvs.getWorkerID(0);
		p.addKVSWorker(lastKVSWorkerAddrPort, lastKVSWorkerID, null);
		p.addKVSWorker(lastKVSWorkerAddrPort, null, firstKVSWorkerID);
		
		// add flameWorkers
		List<String> flameWorkersAddrs = Coordinator.getWorkers();
		for (String addrPort: flameWorkersAddrs) {
			p.addFlameWorker(addrPort);
		}
		Vector<Partition> parts = p.assignPartitions();
		for (Partition part: parts) {
			System.out.println(part);
		}
		// 3. send HTTP request to all flameWorkers
		Thread threads[] = new Thread[parts.size()];
	    String results[] = new String[parts.size()];
	    int resultCodes[] = new int[parts.size()];
		for (int i = 0; i < parts.size(); i++) {
			String flameWorkerAddrPort = parts.elementAt(i).assignedFlameWorker;
			String fromKey = parts.elementAt(i).fromKey;
			String toKey = parts.elementAt(i).toKeyExclusive;
			final String baseUrl = "http://"+flameWorkerAddrPort + operationName;
			// adding the query parameters:
			// Query parameters: this should give you the names of the input and output tables, 
			// the host and port of the KVS coordinator, and the key range the worker should work on
			// inputtable, outputtable, coordinator, fromkey, tokey
			final String rangeUrl = (fromKey==null? "" : ("&fromkey=" + URLEncoder.encode(fromKey, "UTF-8"))) + (toKey==null? "" : ("&tokey=" + URLEncoder.encode(toKey, "UTF-8")));
			// adding optional qparam like the one used in foldByKey
			final String optionalUrl_foldByKey = operationName.equals("/pairrdd/foldByKey")? ("&zeroElement=" + URLEncoder.encode((String)((FlamePairRDDImpl)rddOrPairRDD).tempOptionalParams.get("zeroElement"), "UTF-8")) : "";
			final String optionalUrl_fold = operationName.equals("/rdd/fold")? ("&zeroElement=" + URLEncoder.encode((String)((FlameRDDImpl)rddOrPairRDD).tempOptionalParams.get("zeroElement"), "UTF-8")) : "";
			final String url = baseUrl + "?inputtable=" + URLEncoder.encode(inputRDDOrPairRDDName, "UTF-8") +"&outputtable=" + URLEncoder.encode(outputTableName, "UTF-8")
							+ "&coordinator=" + URLEncoder.encode(kvs.getCoordinator(), "UTF-8") + rangeUrl + optionalUrl_foldByKey + optionalUrl_fold;
			final int j = i;
	        threads[i] = new Thread(operationName + " #"+(i+1)) {
	          public void run() {
	            try {
	              HTTP.Response r = HTTP.doRequest("POST", url, lambda);
	              results[j] = new String(r.body());
	              resultCodes[j] = r.statusCode();
	            } catch (Exception e) {
	              results[j] = "Exception: "+e;
	              e.printStackTrace();
	            }
	          }
	        };
	        threads[i].start();
		}
		// Wait for all the worker to finish operation
		for (int i=0; i<threads.length; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException ie) {
			}
		}
		// print the result for each flameWorker
		System.out.println("\n invokeOperation() after invoking each flameWorkers: operationName=" + operationName); //+ " lambda=" + Serializer.byteArrayToObject(lambda, new File(coordinatorJarName)));
		for (int i = 0; i < parts.size(); i++) {
			System.out.println("flameWorker:" + parts.elementAt(i).assignedFlameWorker + " responseBody='" + results[i] + "' code=" + resultCodes[i]);
		}
		// check response:  and check whether any requests failed or returned status codes other than 200. If so, your function should report this to the caller.
		// TODO Auto-generated method stub

		// 4. returns a new RDD or PairRDD that contains the name of the output table
		// if (kvs.has such table)
		return outputTableName;
		
	}
	
	
	private String getOutputTableType(String operationName) {
		String outputTableType = null;
		switch (operationName) {
		case "/rdd/flapMap":
			outputTableType = "RDD";
			break;
		case "/rdd/mapToPair":
			outputTableType = "PairRDD";
			break;
		case "/rdd/intersection":
			outputTableType = "RDD";
			break;
		case "/rdd/sample":
			outputTableType = "RDD";
			break;
		case "/rdd/groupBy":
			outputTableType = "PairRDD";
			break;
		case "/rdd/distinct":
			outputTableType = "RDD";
			break;
		case "/rdd/fold":
			outputTableType = "RDD";
			break;
		case "/rdd/flapMapToPair":
			outputTableType = "PairRDD";
			break;
		case "/pairrdd/foldByKey":
			outputTableType = "PairRDD";
			break;
		case "/pairrdd/flatMap":
			outputTableType = "RDD";
			break;
		case "/pairrdd/flapMapToPair":
			outputTableType = "PairRDD";
			break;
		case "/pairrdd/join":
			outputTableType = "PairRDD";
			break;
		case "/pairrdd/cogroup":
			outputTableType = "PairRDD";
			break;
		case "/rdd/fromtable":
			outputTableType = "RDD";
			break;
		case "/rdd/filter":
			outputTableType = "RDD";
			break;
		case "/rdd/mapPartitions":
			outputTableType = "RDD";
			break;
	}
		return outputTableType;
	}

	// added private helpers
	// sequenceNum refers to the n-th RDD
	private String generateTableName(int sequenceNum, String tableType) {
		long currentTimeMillis = System.currentTimeMillis();
		return Long.toString(currentTimeMillis) + "_" + sequenceNum + "_" + tableType;
	}

	
	// HW7 added functions
	
	@Override
	public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
		KVSClient kvs = getKVS();
		String outputTableName = this.invokeOperation(new FlameRDDImpl(this, kvs, tableName), "/rdd/fromtable", Serializer.objectToByteArray(lambda));
		FlameRDD flameRDD = new FlameRDDImpl(this, kvs, outputTableName); // might not work until is called by the parent's method
		return flameRDD;
	}

	@Override
	public void setConcurrencyLevel(int keyRangesPerWorkerArg) {
		if (keyRangesPerWorker > 0) {
			this.keyRangesPerWorker = keyRangesPerWorkerArg;
		}
	}
	

}
