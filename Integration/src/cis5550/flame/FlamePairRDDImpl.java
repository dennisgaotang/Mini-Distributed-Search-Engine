package cis5550.flame;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlamePairRDDImpl implements FlamePairRDD {
	public String kvsTableName;
	KVSClient kvs; // for reading from the KVS
	FlameContextImpl flameContext; // for calling instance method invokeOperation()
	public Map<String, Object> tempOptionalParams;

	public FlamePairRDDImpl(FlameContextImpl flameContextImplArg, KVSClient kvsArg, String RDDTableName) {
		flameContext = flameContextImplArg;
		kvsTableName = RDDTableName;
		kvs = kvsArg;
		tempOptionalParams = new HashMap<>();
	}

	@Override
	public List<FlamePair> collect() throws Exception {
		List<FlamePair> elements = new ArrayList<>();
		Iterator<Row> iter = kvs.scan(this.kvsTableName);
		while (iter.hasNext()) {
			Row r = iter.next();
			String rowKey = r.key();
			for (String col: r.columns()) {
				String v = r.get(col);
				elements.add(new FlamePair(rowKey, v));
			}
		}
		return elements;
	}

	@Override
	public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
		tempOptionalParams.put("zeroElement", zeroElement);
		String outputTableName = flameContext.invokeOperation(this, "/pairrdd/foldByKey", Serializer.objectToByteArray(lambda));
		tempOptionalParams.clear();
		FlamePairRDD pairRDD = new FlamePairRDDImpl(flameContext, kvs, outputTableName); // might not work until is called by the parent's method
		return pairRDD;
	}

	// HW7:
	
	@Override
	// this is just like clone the
	public void saveAsTable(String tableNameArg) throws Exception {
		boolean result = kvs.rename(this.kvsTableName, tableNameArg);
		System.out.println("change name from '" + this.kvsTableName + "' to '" + tableNameArg + "' " + (result? "succeeded":"failed"));
		this.kvsTableName = result? tableNameArg : this.kvsTableName;
	}

	@Override
	public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
		String outputTableName = flameContext.invokeOperation(this, "/pairrdd/flatMap", Serializer.objectToByteArray(lambda));
		FlameRDD rdd = new FlameRDDImpl(flameContext, kvs, outputTableName);
		return rdd;
	}

	@Override
	public void destroy() throws Exception {
		kvs.delete(this.kvsTableName);
		this.kvsTableName = null;	
	}

	@Override
	public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
		String outputTableName = flameContext.invokeOperation(this, "/pairrdd/flapMapToPair", Serializer.objectToByteArray(lambda));
		FlamePairRDD flamePairRDD = new FlamePairRDDImpl(flameContext, kvs, outputTableName); // might not work until is called by the parent's method
		return flamePairRDD;
	}

	@Override
	public FlamePairRDD join(FlamePairRDD other) throws Exception {
		// Only another FlamePairRDD's name is passed to workers
		String outputTableName = flameContext.invokeOperation(this, "/pairrdd/join", ((FlamePairRDDImpl)other).kvsTableName.getBytes());
		FlamePairRDD flamePairRDD = new FlamePairRDDImpl(flameContext, kvs, outputTableName); // might not work until is called by the parent's method
		return flamePairRDD;
	}

	@Override
	public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
		// Only another FlamePairRDD's name is passed to workers
		String outputTableName = flameContext.invokeOperation(this, "/pairrdd/cogroup", ((FlamePairRDDImpl)other).kvsTableName.getBytes());
		FlamePairRDD flamePairRDD = new FlamePairRDDImpl(flameContext, kvs, outputTableName); // might not work until is called by the parent's method
		return flamePairRDD;
	}

}
