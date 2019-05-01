package com.graphdb.model;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import org.apache.log4j.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import io.atomix.core.Atomix;
import io.atomix.core.idgenerator.AtomicIdGenerator;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.AtomicMap;
import io.atomix.core.map.AtomicMapBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.utils.time.Versioned;

public class GraphModelImpl<K, V> implements Graph<K, V> {

	private final static Logger logger = Logger.getLogger(GraphModelImpl.class);

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

	public GraphModelImpl(Atomix atomix, String name) {
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

	private long generateId() {
		return relationsIdGenerator.nextId();
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

	public List<Long> addRelation(K from, K to, String type, V value, boolean biDirectional) {

		if (!nodes.containsKey(from) || !nodes.containsKey(to)) {
			return null;
		}

//		Create Relation Object
		Relation<K, V> relation = new Relation<>(generateId(), from, to, value, biDirectional);
		relationsMap.put(relation.getId(), relation);

//		Add Index to the Relation Object
		if (!from2toMap.containsKey(from)) {
			from2toMap.put(from, ArrayListMultimap.create());
		} else {
			from2toMap.get(from).value().put(to, relation.getId());
		}

		if (!from2TypeMap.containsKey(from)) {
			from2TypeMap.put(from, ArrayListMultimap.create());
		} else {
			from2TypeMap.get(from).value().put(type, relation.getId());
		}

		long reverseLookupId;
		if (biDirectional) {
			reverseLookupId = generateId();
			if (!from2toMap.containsKey(to)) {
				from2toMap.put(to, ArrayListMultimap.create());
			} else {
				from2toMap.get(to).value().put(from, reverseLookupId);
			}

			if (!from2TypeMap.containsKey(to)) {
				from2TypeMap.put(to, ArrayListMultimap.create());
			} else {
				from2TypeMap.get(to).value().put(type, reverseLookupId);
			}
			return Arrays.asList(relation.getId(), reverseLookupId);
		}

		return Arrays.asList(relation.getId());
	}

	public Optional<V> getNode(K key) {
		if (nodes.containsKey(key)) {
			return Optional.fromNullable(nodes.get(key).value());
		} else {
			return Optional.absent();
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

	@Override
	public boolean removeNode(K key) {

		boolean error = false;

		if (nodes.containsKey(key)) {
			nodes.remove(key);
		} else {
			logger.error(String.format("Node with key % not found", key));
			error = true;
		}

		// Remove node from both fromMap and typeMap
		if (from2toMap.containsKey(key)) {
			from2toMap.remove(key);
		} else {
			logger.error(String.format("Graph node with key : % not found", key));
			error = true;
		}

		if (from2TypeMap.containsKey(key)) {
			from2TypeMap.remove(key);
		} else {
			logger.error(String.format("Graph type node with key : % not found", key));
			error = true;
		}

		for (Entry<Long, Versioned<Relation>> e : relationsMap.entrySet()) {
			Relation rel = e.getValue().value();
			if (rel.getFrom().equals(key) || rel.getTo().equals(key)) {
				relationsMap.remove(rel.getId());
			}
		}

		if (error) {
			return false;
		}

		return true;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public boolean removeRelation(long id) {
		if (relationsMap.containsKey(id)) {
			Relation relation = relationsMap.get(id).value();
			if (from2toMap.containsKey((K) relation.getFrom())) {
				from2toMap.remove((K) relation.getFrom());
			}
			if (from2TypeMap.containsKey((K) relation.getFrom())) {
				from2TypeMap.remove((K) relation.getFrom());
			}
			relationsMap.remove(id);
			return true;
		}
		logger.error(String.format("Relation id:% not found", id));
		return false;
	}

	@Override
	public boolean removeRelation(K from, K to, String type, boolean biDirectional) {
		logger.info("Inside remove relation by from,to,type");
		boolean error = false;

		if (from2toMap.containsKey(from)) {
			from2toMap.remove(from);
		} else {
			error = true;
			logger.error(String.format("Relation with key: % not found", from));
		}
		if (from2TypeMap.containsKey(from)) {
			Multimap<String, Long> typeMap = from2TypeMap.get(from).value();
			if (typeMap.containsKey(type)) {
				typeMap.removeAll(type);
			}
		} else {
			error = true;
			logger.error(String.format("Relation with key: % not found", from));
		}

		if (biDirectional) {
			if (from2toMap.containsKey(to)) {
				from2toMap.remove(to);
			} else {
				error = true;
				logger.error(String.format("Relation with key: % not found", to));
			}
			if (from2TypeMap.containsKey(to)) {
				Multimap<String, Long> typeMap = from2TypeMap.get(to).value();
				if (typeMap.containsKey(type)) {
					typeMap.removeAll(type);
				}
			} else {
				error = true;
				logger.error(String.format("Relation with key: % not found", to));
			}
		}

		if (error) {
			logger.error("Something catastrophic please DEBUG ME");
			return false;
		}
		return true;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Relation getRelationById(long relationId) {
		if (relationsMap.containsKey(relationId)) {
			return relationsMap.get(relationId).value();
		}
		return null;
	}

	@Override
	public String getRelationType(K from, K to) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getNodeDegree(K key) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getNodeDegree(long id) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public List<Relation<K, V>> getOutgoingRelations(K from) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Relation<K, V>> getOutgoingRelations(long relationId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Long> getOutgoingRelationNodes(K from) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Relation<K, V>> getIncomingRelations(K from) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Relation<K, V>> getIncomingRelations(long relationId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Long> getIncomingRelationNodes(K from) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean areRelated(K from, K to) {
		// TODO Auto-generated method stub
		return false;
	}

}
