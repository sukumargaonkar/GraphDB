package com.graphdb.model;

import java.util.concurrent.CompletableFuture;

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

	private AtomicMapBuilder<K, V> nodesMapBuilder;
	private AtomicMapBuilder<Long, Relation> relationsMapBuilder;
	private AtomicMapBuilder<K, Multimap<String, Long>> from2TypeBuilder;
	private AtomicMapBuilder<K, Multimap<K, Long>> from2ToBuilder;
	private AtomicMap<K, V> nodes;
	private AsyncAtomicMap<K, V> asyncNodes;
	private AtomicMap<Long, Relation> relations;
	private AsyncAtomicMap<Long, Relation> asyncRelations;
	private AtomicMap<K, Multimap<String, Long>> from2Type;
	private AsyncAtomicMap<K, Multimap<String, Long>> AsyncFrom2Type;
	private AtomicMap<K, Multimap<K, Long>> from2to;
	private AsyncAtomicMap<K, Multimap<K, Long>> AsyncFrom2to;

	private AtomicIdGenerator relationsIdGenerator;

	class Relation{
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

		relations = relationsMapBuilder.build();
		from2to = from2ToBuilder.build();
		from2Type = from2TypeBuilder.build();

		asyncNodes = nodes.async();
		asyncRelations = relations.async();
		AsyncFrom2to = from2to.async();
		AsyncFrom2Type = from2Type.async();
	}

	public boolean addNode(K key, V value) {
		if (!nodes.containsKey(key)) {
			nodes.put(key, value);
			return true;
		} else {
			return false;
		}
	}
	public CompletableFuture addNodeAsync(K key, V value) {
			if (!nodes.containsKey(key)) {
				nodes.put(key, value);
				return asyncNodes.put(key, value);
			} else {
				CompletableFuture future = new CompletableFuture();
				future.completeExceptionally(new Exception("Node already exists."));
				return future;
			}
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
			return asyncNodes.get(key);
		}else{
			CompletableFuture future = new CompletableFuture();
			future.completeExceptionally(new Exception("Node already exists."));
			return future;
		}
	}

	public boolean addRelation(K from, K to, String type, V value, boolean biDirectional){

		if(!nodes.containsKey(from) || !nodes.containsKey(to)){
			return false;
		}

//		Create Relation Object
		Relation rel = new Relation(relationsIdGenerator.nextId(), from, to, value, biDirectional);
		relations.put(rel.id, rel);

//		Add Index to the Relation Object
		if(!from2to.containsKey(from))
			from2to.put(from, ArrayListMultimap.create());
		from2to.get(from).value().put(to, rel.id);

		if(!from2Type.containsKey(from))
			from2Type.put(from, ArrayListMultimap.create());
		from2Type.get(from).value().put(type, rel.id);

		return true;
	}

}
