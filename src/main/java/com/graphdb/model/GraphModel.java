package com.graphdb.model;

import java.util.concurrent.CompletableFuture;

import org.apache.log4j.Logger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import io.atomix.core.Atomix;
import io.atomix.core.idgenerator.AtomicIdGenerator;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.AtomicMap;
import io.atomix.core.map.AtomicMapBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.utils.time.Versioned;

public class GraphModel<K, V> {
	
	private final static Logger logger = Logger.getLogger(GraphModel.class);

	private AtomicMapBuilder<K, V> nodesMapBuilder;
	private AtomicMapBuilder<Long, Relation> relationsMapBuilder;
	private AtomicMapBuilder<K, Multimap<String, Long>> from2TypeBuilder;
	private AtomicMapBuilder<K, Multimap<K, Long>> from2ToBuilder;
	private AtomicMap<K, V> nodes;
	private AsyncAtomicMap<K, V> asyncNodesMap;
	private AtomicMap<Long, Relation> relationsMap;
	private AsyncAtomicMap<Long, Relation> asyncRelationsMap;
	private AtomicMap<K, Multimap<String, Long>> from2TypeMap;
	private AsyncAtomicMap<K, Multimap<String, Long>> asyncFrom2TypeMap;
	private AtomicMap<K, Multimap<K, Long>> from2toMap;
	private AsyncAtomicMap<K, Multimap<K, Long>> asyncFrom2ToMap;

	private static AtomicIdGenerator relationsIdGenerator;

	private class Relation {
		long id;
		K from;
		K to;
		V data;
		boolean biDirectional;

		public Relation(long id, K from, K to, V data, boolean biDirectional) {
			this.id = id;
			this.from = from;
			this.to = to;
			this.data = data;
			this.biDirectional = biDirectional;
		}
	}

	public GraphModel(Atomix atomix, String name) {
		nodesMapBuilder = atomix.<K, V>atomicMapBuilder(name + "_nodes").withCacheEnabled();
		relationsMapBuilder = atomix.<Long, Relation>atomicMapBuilder(name + "_relations").withCacheEnabled();
		from2ToBuilder = atomix.<K, Multimap<K, Long>>atomicMapBuilder(name + "_from2To").withCacheEnabled();
		from2TypeBuilder = atomix.<K, Multimap<String, Long>>atomicMapBuilder(name + "_from2Type").withCacheEnabled();
		relationsIdGenerator = atomix.getAtomicIdGenerator("relations_id_generator");
	}

	public void withProtocol(ProxyProtocol protocol) {
		nodesMapBuilder.withProtocol(protocol);
		relationsMapBuilder.withProtocol(protocol);
	}

	public void withCacheSize(int size) {
		nodesMapBuilder.withCacheSize(size);
		relationsMapBuilder.withCacheSize(size);
	}

	public void setReadOnly() {
		nodesMapBuilder.withReadOnly();
		relationsMapBuilder.withReadOnly();
	}

	public void buildAtomicMultiMap() {
		nodes = nodesMapBuilder.build();

		relationsMap = relationsMapBuilder.build();
		from2toMap = from2ToBuilder.build();
		from2TypeMap = from2TypeBuilder.build();

		asyncNodesMap = nodes.async();
		asyncRelationsMap = relationsMap.async();
		asyncFrom2ToMap = from2toMap.async();
		asyncFrom2TypeMap = from2TypeMap.async();
	}

	public boolean addNode(K key, V value) {
		if (!nodes.containsKey(key)) {
			nodes.put(key, value);
			return true;
		} else {
			return false;
		}
	}

	public CompletableFuture<?> addNodeAsync(K key, V value) {
		if (!nodes.containsKey(key)) {
			nodes.put(key, value);
			return asyncNodesMap.put(key, value);
		} else {
			CompletableFuture<?> future = new CompletableFuture();
			future.completeExceptionally(new Exception("Node already exists."));
			return future;
		}
	}

	public boolean addRelation(K from, K to, String type, V value, boolean biDirectional) {

		if (!nodes.containsKey(from) || !nodes.containsKey(to)) {
			return false;
		}

//		Create Relation Object
		Relation relation = new Relation(relationsIdGenerator.nextId(), from, to, value, biDirectional);
		relationsMap.put(relation.id, relation);

//		Add Index to the Relation Object
		if (!from2toMap.containsKey(from))
			from2toMap.put(from, ArrayListMultimap.create());
		from2toMap.get(from).value().put(to, relation.id);

		if (!from2TypeMap.containsKey(from))
			from2TypeMap.put(from, ArrayListMultimap.create());
		from2TypeMap.get(from).value().put(type, relation.id);

		return true;
	}

	public V getNode(K key) {
		if (nodes.containsKey(key)) {
			return nodes.get(key).value();
		} else {
			return null;
		}
	}

	public CompletableFuture<Versioned<V>> getNodeAsync(K key) {
		if (nodes.containsKey(key)) {
			return asyncNodesMap.get(key);
		} else {
			CompletableFuture<Versioned<V>> future = new CompletableFuture<Versioned<V>>();
			future.completeExceptionally(new Exception("Node already exists."));
			return future;
		}
	}

}
