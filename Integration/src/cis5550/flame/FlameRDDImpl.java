package cis5550.flame;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlameRDDImpl implements FlameRDD {
	public String kvsTableName;
	KVSClient kvs; // for reading from the KVS
	FlameContextImpl flameContext; // for calling instance method invokeOperation()
	public Map<String, Object> tempOptionalParams;
	int numDistinctMethodCalled;
	
	public FlameRDDImpl(FlameContextImpl flameContextImplArg, KVSClient kvsArg, String RDDTableName) {
		flameContext = flameContextImplArg;
		kvsTableName = RDDTableName;
		kvs = kvsArg;
		tempOptionalParams = new HashMap<>();
		numDistinctMethodCalled = 0;
	}

	@Override
	public List<String> collect() throws Exception {
		List<String> elements = new ArrayList<>();
		Iterator<Row> iter = kvs.scan(this.kvsTableName);
		while (iter.hasNext()) {
			Row r = iter.next();
			elements.add(r.get("value"));
		}
		return elements;
	}

	@Override
	public FlameRDD flatMap(StringToIterable lambda) throws Exception {
		String outputTableName = flameContext.invokeOperation(this, "/rdd/flapMap", Serializer.objectToByteArray(lambda));
		FlameRDD flameRDD = new FlameRDDImpl(flameContext, kvs, outputTableName); // might not work until is called by the parent's method
		return flameRDD;
	}

	@Override
	public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
		String outputTableName = flameContext.invokeOperation(this, "/rdd/mapToPair", Serializer.objectToByteArray(lambda));
		FlamePairRDD pairRDD = new FlamePairRDDImpl(flameContext, kvs, outputTableName); // might not work until is called by the parent's method
		return pairRDD;
	}

	// EC
	
	@Override
	public FlameRDD intersection(FlameRDD r) throws Exception {
		// sending another FlameRDD in the request body
		String outputTableName = flameContext.invokeOperation(this, "/rdd/intersection", ((FlameRDDImpl)r).kvsTableName.getBytes());
		FlameRDD flameRDD = new FlameRDDImpl(flameContext, kvs, outputTableName); // might not work until is called by the parent's method
		// as an coordinator cancel repetition
		((FlameRDDImpl) flameRDD).removeDuplicateValues();
		return flameRDD;
	}

	private void removeDuplicateValues() throws Exception {
		Map<String, String> reverseIndex = new HashMap<>();
		Iterator<Row> iter = this.kvs.scan(this.kvsTableName);
		while (iter.hasNext()) {
			Row r = iter.next();
			String v = r.get("value");
			if (!reverseIndex.containsKey(v)) {
				reverseIndex.put(v, r.key());
			}
		}
		// delete the original table and create a new one with the same name
		kvs.delete(kvsTableName);
		for (Map.Entry<String, String> e: reverseIndex.entrySet()) {
			kvs.put(kvsTableName, e.getValue(), "value", e.getKey().getBytes());
		}
	}

	@Override
	public FlameRDD sample(double f) throws Exception {
		String doubleString = String.valueOf(f);
		String outputTableName = flameContext.invokeOperation(this, "/rdd/sample", doubleString.getBytes());
		FlameRDD flameRDD = new FlameRDDImpl(flameContext, kvs, outputTableName); // might not work until is called by the parent's method
		return flameRDD;
	}

	@Override
	public FlamePairRDD groupBy(StringToString lambda) throws Exception {
		String outputTableName = flameContext.invokeOperation(this, "/rdd/groupBy", Serializer.objectToByteArray(lambda));
		FlamePairRDD pairRDD = new FlamePairRDDImpl(flameContext, kvs, outputTableName); // might not work until is called by the parent's method
		return pairRDD;
	}

	// HW7
	
	@Override
	public int count() throws Exception {
		int count = kvs.count(this.kvsTableName);
		return count;
	}

	@Override
	// change the table name but not copy it
	public void saveAsTable(String tableNameArg) throws Exception {
		boolean result = kvs.rename(this.kvsTableName, tableNameArg);
		System.out.println("change name from '" + this.kvsTableName + "' to '" + tableNameArg + "' " + (result? "succeeded":"failed"));
		this.kvsTableName = result? tableNameArg : this.kvsTableName;
	}

	@Override
	public FlameRDD distinct() throws Exception {
		String outputTableName = flameContext.invokeOperation(this, "/rdd/distinct", null);
		FlameRDD flameRDD = new FlameRDDImpl(flameContext, kvs, outputTableName); // might not work until is called by the parent's method
		return flameRDD;
	}

	@Override
	public void destroy() throws Exception {
		kvs.delete(this.kvsTableName);
		this.kvsTableName = null;
	}

	@Override
	public Vector<String> take(int num) throws Exception {
		Vector<String> firstN = new Vector<>();
		Iterator<Row> iter = kvs.scan(this.kvsTableName);
		while (num > 0 && iter.hasNext()) {
			Row r = iter.next();
			firstN.add(r.get("value"));
			num--;
		}
		return firstN;
	}

	@Override
	public String fold(String zeroElement, TwoStringsToString lambda) throws Exception {
		tempOptionalParams.put("zeroElement", zeroElement);
		String outputTableName = flameContext.invokeOperation(this, "/rdd/fold", Serializer.objectToByteArray(lambda));
		tempOptionalParams.clear();
		// aggregate the result, the outputTableName indicates the accumulators from various workers
		// extracting the result from the kvs and delete the table
		String accumulator = zeroElement;
		Iterator<Row> iter = kvs.scan(outputTableName);
		while (iter.hasNext()) {
			Row r = iter.next();
			String v = r.get("value");
			System.out.println("fold " + r.key() + " = " + v);
			accumulator = lambda.op(accumulator, v);
		}
		// delete the temporary output table
		kvs.delete(outputTableName);
		return accumulator;
	}

	@Override
	public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
		String outputTableName = flameContext.invokeOperation(this, "/rdd/flapMapToPair", Serializer.objectToByteArray(lambda));
		FlamePairRDD flamePairRDD = new FlamePairRDDImpl(flameContext, kvs, outputTableName); // might not work until is called by the parent's method
		return flamePairRDD;
	}

	@Override
	public FlameRDD filter(StringToBoolean lambda) throws Exception {
		String outputTableName = flameContext.invokeOperation(this, "/rdd/filter", Serializer.objectToByteArray(lambda));
		FlameRDD flameRDD = new FlameRDDImpl(flameContext, kvs, outputTableName); // might not work until is called by the parent's method
		return flameRDD;
	}

	@Override
	public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
		String outputTableName = flameContext.invokeOperation(this, "/rdd/mapPartitions", Serializer.objectToByteArray(lambda));
		FlameRDD flameRDD = new FlameRDDImpl(flameContext, kvs, outputTableName); // might not work until is called by the parent's method
		return flameRDD;
	}
	


}
