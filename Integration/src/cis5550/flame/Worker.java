package cis5550.flame;

import java.util.*;
import java.net.*;
import java.io.*;
import java.util.Iterator;

import static cis5550.webserver.Server.*;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.Serializer;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.PairToStringIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.IteratorToIterator;
import cis5550.flame.FlameRDD.StringToBoolean;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.flame.FlameRDD.StringToPairIterable;
import cis5550.flame.FlameRDD.StringToString;
import cis5550.kvs.*;
import cis5550.webserver.Request;

class Worker extends cis5550.generic.Worker {

	private static final Logger logger
			= Logger.getLogger(Worker.class);
	public static class RowToStringIterator implements Iterator<String> {
	    private Iterator<Row> rowIterator;

	    public RowToStringIterator(Iterator<Row> rowIterator) {
	        this.rowIterator = rowIterator;
	    }

	    @Override
	    public boolean hasNext() {
	        return rowIterator.hasNext();
	    }

	    @Override
	    public String next() {
	        // Assuming Row class has a getValue() method that returns a String
	        Row row = rowIterator.next();
	        return row.get("value");
	    }

	    @Override
	    public void remove() {
	        throw new UnsupportedOperationException("Remove operation is not supported");
	    }
	}
	
	public static void main(String args[]) throws IOException {
    if (args.length != 2) {
    	System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
    	System.exit(1);
    }

    int port = Integer.parseInt(args[0]);
    String server = args[1];
	  startPingThread(server, ""+port, port);
    final File myJAR = new File("__worker"+port+"-current.jar");

  	port(port);

    post("/useJAR", (request,response) -> {
      FileOutputStream fos = new FileOutputStream(myJAR);
      fos.write(request.bodyAsBytes());
      fos.close();
      return "OK";
    });
    
    post("/rdd/flapMap", (req,res) -> {
        // gather request inputs inputtable, outputtable, coordinator, fromkey, tokey
    	String inputtable = req.queryParams("inputtable");
    	String outputtable = req.queryParams("outputtable");
    	String coordinator = req.queryParams("coordinator");
    	String fromkey = req.queryParams("fromkey");
    	String tokey = req.queryParams("tokey");
    	StringToIterable lambda = (StringToIterable) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
    	
    	KVSClient kvs = new KVSClient(coordinator);
    	Iterator<Row> iter = kvs.scan(inputtable, fromkey, tokey);
    	while (iter.hasNext()) {
			Row oldRow = iter.next();
			String oldValue = oldRow.get("value");
//			Iterable<String> newValues = lambda.op(oldValue);
			Iterable<String> newValues = lambda != null? lambda.op(oldValue) : null;
			if (newValues == null ) {
				continue;
			}
			int sequenceNum = 1;
			for (String newValue: newValues) {
				Row row = new Row(oldRow.key() + "_" + Integer.toString(sequenceNum));
				sequenceNum++;
				row.put("value", newValue);
				kvs.putRow(outputtable, row);
			}
		}
        return "OK";
      });
    
    post("/pairrdd/flatMap", (req,res) -> {
        // gather request inputs inputtable, outputtable, coordinator, fromkey, tokey
    	String inputtable = req.queryParams("inputtable");
    	String outputtable = req.queryParams("outputtable");
    	String coordinator = req.queryParams("coordinator");
    	String fromkey = req.queryParams("fromkey");
    	String tokey = req.queryParams("tokey");
    	PairToStringIterable lambda = (PairToStringIterable) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
    	
    	KVSClient kvs = new KVSClient(coordinator);
    	Iterator<Row> iter = kvs.scan(inputtable, fromkey, tokey);
    	while (iter.hasNext()) {
			Row oldRow = iter.next();
			for (String col: oldRow.columns()) {
				FlamePair p = new FlamePair(oldRow.key(), oldRow.get(col));
//				Iterable<String> newValues = lambda.op(p);
				Iterable<String> newValues = lambda != null? lambda.op(p) : null;
				if (newValues == null ) {
					continue;
				}
				int sequenceNum = 1;
				for (String newValue: newValues) {
					String rKey = oldRow.key() + "_" + col + "_" + Integer.toString(sequenceNum);
					sequenceNum++;
					kvs.put(outputtable, rKey, "value", newValue.getBytes());
				}
			}
		}
        return "OK";
      });
    
    post("/rdd/mapToPair", (req,res) -> {
        // gather request inputs inputtable, outputtable, coordinator, fromkey, tokey
    	String inputtable = req.queryParams("inputtable");
    	String outputtable = req.queryParams("outputtable");
    	String coordinator = req.queryParams("coordinator");
    	String fromkey = req.queryParams("fromkey");
    	String tokey = req.queryParams("tokey");
    	StringToPair lambda = (StringToPair) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
    	
    	KVSClient kvs = new KVSClient(coordinator);
    	Iterator<Row> iter = kvs.scan(inputtable, fromkey, tokey);
    	while (iter.hasNext()) {
			Row oldRow = iter.next();
			String oldValue = oldRow.get("value");
//			FlamePair newPair = lambda.op(oldValue);
			FlamePair newPair = lambda != null? lambda.op(oldValue) : null;
			if (newPair != null ) {
				kvs.put(outputtable, newPair._1(), oldRow.key(), newPair._2().getBytes());
			}
		}
        return "OK";
      });
    
    post("/rdd/distinct", (req,res) -> {
        // gather request inputs inputtable, outputtable, coordinator, fromkey, tokey
    	String inputtable = req.queryParams("inputtable");
    	String outputtable = req.queryParams("outputtable");
    	String coordinator = req.queryParams("coordinator");
    	String fromkey = req.queryParams("fromkey");
    	String tokey = req.queryParams("tokey");
    	
    	KVSClient kvs = new KVSClient(coordinator);
    	Iterator<Row> iter = kvs.scan(inputtable, fromkey, tokey);
		while (iter.hasNext()) {
			Row r = iter.next();
			String v = r.get("value");
			kvs.put(outputtable, v, "value", v.getBytes());
		}
        return "OK";
      });
    
    post("/pairrdd/foldByKey", (req,res) -> {
        // gather request inputs inputtable, outputtable, coordinator, fromkey, tokey
    	String inputtable = req.queryParams("inputtable");
    	String outputtable = req.queryParams("outputtable");
    	String coordinator = req.queryParams("coordinator");
    	String fromkey = req.queryParams("fromkey");
    	String tokey = req.queryParams("tokey");
    	String zeroElement = req.queryParams("zeroElement");
    	TwoStringsToString lambda = (TwoStringsToString) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
    	
    	
    	KVSClient kvs = new KVSClient(coordinator);
    	Iterator<Row> iter = kvs.scan(inputtable, fromkey, tokey);
    	while (iter.hasNext()) {
			Row oldRow = iter.next();
			String k = oldRow.key();
			String accumulator = zeroElement;
			for (String col: oldRow.columns()) {
				String vi = oldRow.get(col);
//				accumulator = lambda.op(accumulator, vi);
				accumulator = lambda != null?  lambda.op(accumulator, vi) : null;
			}
			String v = accumulator;
			kvs.put(outputtable, k, "value", v.getBytes());
		}
        return "OK";
      });
    
    post("/rdd/fold", (req,res) -> {
        // gather request inputs inputtable, outputtable, coordinator, fromkey, tokey
    	String inputtable = req.queryParams("inputtable");
    	String outputtable = req.queryParams("outputtable");
    	String coordinator = req.queryParams("coordinator");
    	String fromkey = req.queryParams("fromkey");
    	String tokey = req.queryParams("tokey");
    	String zeroElement = req.queryParams("zeroElement");
//    	System.out.println("foldworker received zeroElement = " + zeroElement);
    	TwoStringsToString lambda = (TwoStringsToString) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
    	
    	
    	KVSClient kvs = new KVSClient(coordinator);
    	Iterator<Row> iter = kvs.scan(inputtable, fromkey, tokey);
    	String accumulator = zeroElement;
    	String lastRowKey = null;
    	if (!iter.hasNext()) return "OK";
    	while (iter.hasNext()) {
			Row oldRow = iter.next();
			lastRowKey = oldRow.key();
			String v = oldRow.get("value");
//			accumulator = lambda.op(accumulator, v);
			accumulator = lambda != null?  lambda.op(accumulator, v) : null;

		}
    	kvs.put(outputtable, "workerOnPort_" + port + lastRowKey, "value", accumulator.getBytes());
        return "OK";
      });
    
    
    // HW6 EC
    
    post("/rdd/intersection", (req,res) -> {
        // gather request inputs inputtable, outputtable, coordinator, fromkey, tokey
    	String inputtable = req.queryParams("inputtable");
    	String outputtable = req.queryParams("outputtable");
    	String coordinator = req.queryParams("coordinator");
    	String fromkey = req.queryParams("fromkey");
    	String tokey = req.queryParams("tokey");
    	String anotherRDDName = req.body();
    	
    	
    	KVSClient kvs = new KVSClient(coordinator);
    	Set<String> values1 = collectDistinct(kvs, inputtable, fromkey, tokey);
    	// get all the values with all range for another RDD
    	Set<String> values2 = collectDistinct(kvs, anotherRDDName, null, null);
    	values1.retainAll(values2);
    	int seed = 1;
    	for (String v: values1) {
    		kvs.put(outputtable, generateUniqueRandomRowKey(seed+port), "value", v.getBytes());
    		seed++;
    	}
        return "OK";
      });

	post("/rdd/sample", (req,res) -> {
	        // gather request inputs inputtable, outputtable, coordinator, fromkey, tokey
	    	String inputtable = req.queryParams("inputtable");
	    	String outputtable = req.queryParams("outputtable");
	    	String coordinator = req.queryParams("coordinator");
	    	String fromkey = req.queryParams("fromkey");
	    	String tokey = req.queryParams("tokey");
	    	double prob = Double.parseDouble(req.body());
	    	
	    	KVSClient kvs = new KVSClient(coordinator);
	    	Iterator<Row> iter = kvs.scan(inputtable, fromkey, tokey);
	    	Random random = new Random();
	    	while (iter.hasNext()) {
				Row oldRow = iter.next();
				String v = oldRow.get("value");
				if (random.nextDouble() < prob) {
	                kvs.put(outputtable, oldRow.key(), "value", v.getBytes());
	            }			
			}
	        return "OK";
	      });
	
	post("/rdd/groupBy", (req,res) -> {
        // gather request inputs inputtable, outputtable, coordinator, fromkey, tokey
    	String inputtable = req.queryParams("inputtable");
    	String outputtable = req.queryParams("outputtable");
    	String coordinator = req.queryParams("coordinator");
    	String fromkey = req.queryParams("fromkey");
    	String tokey = req.queryParams("tokey");
    	StringToString lambda = (StringToString) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
    	
    	KVSClient kvs = new KVSClient(coordinator);
    	Iterator<Row> iter = kvs.scan(inputtable, fromkey, tokey);
    	Map<String, String> result = new HashMap<>();
    	while (iter.hasNext()) {
			Row oldRow = iter.next();
			String oldValue = oldRow.get("value");
//			String k = lambda.op(oldValue);
			String k = lambda != null?  lambda.op(oldValue) : null;
			if (result.containsKey(k)) {
				result.put(k, result.get(k) + "," + oldValue);
			} else {
				result.put(k, oldValue);
			}			
		}
    	// put the RDDPairTable
    	for (Map.Entry<String, String> e: result.entrySet()) {
			kvs.put(outputtable, e.getKey(), "csvStrings", e.getValue().getBytes());
		}
        return "OK";
      });
	
	// additional routes HW7
	post("/rdd/fromtable", (req,res)->{
		// gather request inputs inputtable, outputtable, coordinator, fromkey, tokey
    	String inputtable = req.queryParams("inputtable");
    	String outputtable = req.queryParams("outputtable");
    	String coordinator = req.queryParams("coordinator");
    	String fromkey = req.queryParams("fromkey");
    	String tokey = req.queryParams("tokey");
    	RowToString lambda = (RowToString) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);

//		System.out.println("inputtable: " + inputtable);
//		System.out.println("outputtable: " + outputtable);
//		System.out.println("coordinator: " + coordinator);
//		System.out.println("fromkey: " + fromkey);
//		System.out.println("tokey: " + tokey);


    	KVSClient kvs = new KVSClient(coordinator);
    	Iterator<Row> iter = kvs.scan(inputtable, fromkey, tokey);
    	while (iter.hasNext()) {
			Row oldRow = iter.next();
			String oldKey = oldRow.key();
//			String newValue = lambda.op(oldRow);
			String newValue = lambda != null? lambda.op(oldRow) : null;
			if (newValue == null ) {
				continue;
			}

//			System.out.println("kvs put in flame worker: ");
//			System.out.println("outputtable: " + outputtable);
//			System.out.println("oldKey: " + oldKey);
//			System.out.println("newValue: " + newValue);
			// adding to new RDD
			kvs.put(outputtable, oldKey, "value", newValue.getBytes());
		}
        return "OK";
	});
	
	post("/rdd/flapMapToPair", (req,res) -> {
        // gather request inputs inputtable, outputtable, coordinator, fromkey, tokey
    	String inputtable = req.queryParams("inputtable");
    	String outputtable = req.queryParams("outputtable");
    	String coordinator = req.queryParams("coordinator");
    	String fromkey = req.queryParams("fromkey");
    	String tokey = req.queryParams("tokey");
    	StringToPairIterable lambda = (StringToPairIterable) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
    	
    	KVSClient kvs = new KVSClient(coordinator);
    	Iterator<Row> iter = kvs.scan(inputtable, fromkey, tokey);
    	while (iter.hasNext()) {
			Row oldRow = iter.next();
			String oldValue = oldRow.get("value");
//			Iterable<FlamePair> newPairs = lambda.op(oldValue);
			Iterable<FlamePair> newPairs = (lambda != null) ? lambda.op(oldValue) : null;
			if (newPairs == null ) {
				continue;
			}
			int sequenceNum = 1;
			for (FlamePair p: newPairs) {
				String colKey = oldRow.key() + "_" + Integer.toString(sequenceNum);		
				sequenceNum++;
				if (p != null ) {
					// the row key is taken as p._1(), supposed to be the same as the oldRow.key()
					kvs.put(outputtable, p._1(), colKey, p._2().getBytes());
				}
			}
		}
        return "OK";
      });
	
	post("/pairrdd/flapMapToPair", (req,res) -> {
        // gather request inputs inputtable, outputtable, coordinator, fromkey, tokey
    	String inputtable = req.queryParams("inputtable");
    	String outputtable = req.queryParams("outputtable");
    	String coordinator = req.queryParams("coordinator");
    	String fromkey = req.queryParams("fromkey");
    	String tokey = req.queryParams("tokey");
    	PairToPairIterable lambda = (PairToPairIterable) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
    	
    	KVSClient kvs = new KVSClient(coordinator);
    	Iterator<Row> iter = kvs.scan(inputtable, fromkey, tokey);
    	while (iter.hasNext()) {
			Row oldRow = iter.next();
			for (String col: oldRow.columns()) {
				FlamePair p = new FlamePair(oldRow.key(), oldRow.get(col));
//				Iterable<FlamePair> newPairValues = lambda.op(p);
				Iterable<FlamePair> newPairValues = lambda != null? lambda.op(p) : null;

				if (newPairValues == null ) {
					continue;
				}
				int sequenceNum = 1;
				for (FlamePair newPair: newPairValues) {
					String colKey = oldRow.key() + "_" + col + "_" + Integer.toString(sequenceNum);
					sequenceNum++;
					if (p != null ) {
						// the row key is taken as p._1(), supposed to be the same as the oldRow.key()
						kvs.put(outputtable, newPair._1(), colKey, newPair._2().getBytes());
					}
				}
			}
		}
        return "OK";
      });
	
	post("/pairrdd/join", (req,res) -> {
        // gather request inputs inputtable, outputtable, coordinator, fromkey, tokey
    	String inputtable = req.queryParams("inputtable");
    	String outputtable = req.queryParams("outputtable");
    	String coordinator = req.queryParams("coordinator");
    	String fromkey = req.queryParams("fromkey");
    	String tokey = req.queryParams("tokey");
    	String anotherPairRDDName = new String(req.bodyAsBytes());
    	
    	KVSClient kvs = new KVSClient(coordinator);
    	Iterator<Row> iter = kvs.scan(inputtable, fromkey, tokey);
    	while (iter.hasNext()) {
			Row row1 = iter.next();
			String k1 = row1.key();
			if (kvs.existsRow(anotherPairRDDName, k1)) {
				Row row2 = kvs.getRow(anotherPairRDDName, k1);
				for (String col1: row1.columns()) {
					String v1 = row1.get(col1);
					for (String col2: row2.columns()) {
						String v2 = row2.get(col2);
						String newColName = col1 + "&" + col2;
						kvs.put(outputtable, k1, newColName, v1 + "," + v2);
 					}
				}
			}
		}
        return "OK";
      });
	
	// HW7 EC
	post("/rdd/filter", (req,res) -> {
        // gather request inputs inputtable, outputtable, coordinator, fromkey, tokey
    	String inputtable = req.queryParams("inputtable");
    	String outputtable = req.queryParams("outputtable");
    	String coordinator = req.queryParams("coordinator");
    	String fromkey = req.queryParams("fromkey");
    	String tokey = req.queryParams("tokey");
    	StringToBoolean lambda = (StringToBoolean) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
    	
    	KVSClient kvs = new KVSClient(coordinator);
    	Iterator<Row> iter = kvs.scan(inputtable, fromkey, tokey);
    	if (iter == null) return "OK";
    	while (iter.hasNext()) {
			Row r = iter.next();
			String k = r.key();
			String v = r.get("value");
//			boolean predicate = lambda.op(v);
			boolean predicate = lambda != null && lambda.op(v);

			if (predicate) {
				kvs.put(outputtable, k, "value", v.getBytes());
			}
		}
        return "OK";
      });
      
	post("/rdd/mapPartitions", (req,res) -> {
        // gather request inputs inputtable, outputtable, coordinator, fromkey, tokey
    	String inputtable = req.queryParams("inputtable");
    	String outputtable = req.queryParams("outputtable");
    	String coordinator = req.queryParams("coordinator");
    	String fromkey = req.queryParams("fromkey");
    	String tokey = req.queryParams("tokey");
    	IteratorToIterator lambda = (IteratorToIterator) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
    	
    	KVSClient kvs = new KVSClient(coordinator);
    	Iterator<Row> iter = kvs.scan(inputtable, fromkey, tokey);
    	RowToStringIterator strIter = new RowToStringIterator(iter);
//    	Iterator<String> newIter = lambda.op(strIter);
		Iterator<String> newIter = lambda != null? lambda.op(strIter) : null;
    	if (newIter == null) {System.out.println("The newIter is null"); return "OK";}
    	int seq = 1;
    	while (newIter.hasNext()) {
			String s = newIter.next();
			kvs.put(outputtable, generateUniqueRandomRowKey(seq++), "value", s.getBytes());
		}
        return "OK";
      });
	
	post("/pairrdd/cogroup", (req,res) -> {
        // gather request inputs inputtable, outputtable, coordinator, fromkey, tokey
    	String inputtable = req.queryParams("inputtable");
    	String outputtable = req.queryParams("outputtable");
    	String coordinator = req.queryParams("coordinator");
    	String fromkey = req.queryParams("fromkey");
    	String tokey = req.queryParams("tokey");
    	String anotherPairRDDName = new String(req.bodyAsBytes());
    	
    	KVSClient kvs = new KVSClient(coordinator);
    	Iterator<Row> iter1 = kvs.scan(inputtable, fromkey, tokey);
    	while (iter1.hasNext()) {
			Row row1 = iter1.next();
			String k1 = row1.key();
			if (kvs.existsRow(anotherPairRDDName, k1)) {
				// for keys in both pairRDD
				Row row2 = kvs.getRow(anotherPairRDDName, k1);
				String v = "[" + combineRowValuesAsCSV(row1) + "],[" + combineRowValuesAsCSV(row2) + "]";
				kvs.put(outputtable, k1, "result", v.getBytes());
			} else {
				// for keys in just pairRDD1
				String v = "[" + combineRowValuesAsCSV(row1) + "],[]";
				kvs.put(outputtable, k1, "result", v.getBytes());
			}
			// for keys just in pairRDD2
			Iterator<Row> iter2 = kvs.scan(inputtable, fromkey, tokey);
			while (iter2.hasNext()) {
				Row row2 = iter2.next();
				String k2 = row2.key();
				if (kvs.existsRow(inputtable, k2)) continue; // skip those keys in both table
				// for keys just in pairRDD2
				String v = "[],[" + combineRowValuesAsCSV(row2) + "]";
				kvs.put(outputtable, k2, "result", v.getBytes());
			}
		}
        return "OK";
      });
	
	// 
	}
	
	
	private static String combineRowValuesAsCSV(Row r) throws Exception {
		String csv = "";
		for (String c: r.columns()) {
			String v = r.get(c);
			csv = csv + (csv.equals("") ? "" : ",") + v;
		}
		return csv;
	}
	
	private static Set<String> collectDistinct(KVSClient kvs, String kvsTableName, String from, String to) throws Exception {
		Set<String> elements = new HashSet<>();
		Iterator<Row> iter = kvs.scan(kvsTableName, from, to);
		while (iter.hasNext()) {
			Row r = iter.next();
			elements.add(r.get("value"));
		}
		return elements;
	}
	
	private static String generateUniqueRandomRowKey(int seed) {
		return Hasher.hash(Integer.toString(seed));
	}
}
