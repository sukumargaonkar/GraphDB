package com.graphdb.model;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import io.atomix.core.Atomix;
import io.atomix.core.idgenerator.AtomicIdGenerator;
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
	private AtomicMap<Long, Relation> relationsMap;
	private AtomicMap<K, Multimap<String, Long>> from2TypeMap;
	private AtomicMap<K, Multimap<K, Long>> from2toMap;

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
		logger.info("Building AtomicMultiMap");
		nodes = nodesMapBuilder.build();

		relationsMap = relationsMapBuilder.build();
		from2toMap = from2ToBuilder.build();
		from2TypeMap = from2TypeBuilder.build();
	}

	private long generateId() {
		return relationsIdGenerator.nextId();
	}

	public boolean addNode(K key, V value) {

		logger.info("Inside addNode");
		if (!nodes.containsKey(key)) {
			nodes.put(key, value);
			return true;
		} else {
			return false;
		}
	}

	public List<Long> addRelation(K from, K to, String type, V value, boolean biDirectional) {

		logger.info("Inside addRelation");

		if (!nodes.containsKey(from) || !nodes.containsKey(to)) {
			return null;
		}

//		Create Relation Object
		Relation<K, V> relation = new Relation<>(generateId(), from, to, value);
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
		logger.info("Inside getNode");
		if (nodes.containsKey(key)) {
			return Optional.fromNullable(nodes.get(key).value());
		} else {
			return Optional.absent();
		}
	}

	@Override
	public boolean removeNode(K key) {
		logger.info("Inside removeNode");

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
		logger.info("Inside removeRelation");
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
		logger.info("Inside getRelationById");
		if (relationsMap.containsKey(relationId)) {
			return relationsMap.get(relationId).value();
		}
		return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public List<String> getRelationType(K from, K to) {
		logger.info("Inside getRelationType");
		List<String> relationType = Lists.newArrayList();
		for (Entry e : relationsMap.entrySet()) {
			Relation rel = (Relation) e.getValue();
			if (rel.getFrom().equals(from) && rel.getTo().equals(to)) {
				if (from2TypeMap.containsKey(from)) {
					Multimap<String, Long> relMap = from2TypeMap.get(from).value();
					for (Map.Entry entry : relMap.asMap().entrySet()) {
						if (entry.getValue().equals(rel.getId())) {
							relationType.add((String) entry.getKey());
						}
					}
				}
			}
		}
		return null;
	}

	/*
	 * calculate outdegree given a node
	 */
	@Override
	public long getNodeDegree(K key) {
		logger.info("Inside getNodeDegree");

		if (from2toMap.containsKey(key)) {
			Map<K, Collection<Long>> fromMap = from2toMap.get(key).value().asMap();
			return fromMap.get(key).size();
		} else {
			logger.error(String.format("Node with key : % not found", key));
		}
		return -1;
	}

	@Override
	public List<Relation> getOutgoingRelations(K from) {
		logger.info("Inside getOutgoingRelations");
		List<Relation> relations = Lists.newArrayList();
		if (from2toMap.containsKey(from)) {
			Multimap<K, Long> fromMap = from2toMap.get(from).value();
			List<Long> relationIdList = (List<Long>) fromMap.values();
			relationIdList.stream().forEach(e -> relations.add(relationsMap.get(e).value()));
			return relations;
		}
		return null;
	}

	@Override
	public List<Relation> getOutgoingRelations(K from, String type) {
		logger.info("Inside getOutgoingRelationNodes");
		List<Relation> relationsList = Lists.newArrayList();
		if (from2toMap.containsKey(from)) {
			Multimap<String, Long> fromMap = from2TypeMap.get(from).value();
			if (fromMap.containsKey(type)) {
				List<Long> fromList = (List<Long>) fromMap.values();
				fromList.stream().forEach(t -> relationsList.add(relationsMap.get(t).value()));
				return relationsList;
			}
		}
		return null;
	}

	@Override
	public List<Relation> getIncomingRelations(K from) {
		logger.info("Inside getIncomingRelations");
		List<Relation> relationsList = Lists.newArrayList();
		for (Entry<K, Versioned<Multimap<K, Long>>> e : from2toMap.entrySet()) {
			Multimap<K, Long> toMap = e.getValue().value();
			if (toMap.containsKey(from)) {
				List<Long> toList = (List<Long>) toMap.get(from);
				toList.stream().forEach(t -> relationsList.add(relationsMap.get(t).value()));
			}
		}
		return relationsList.size() == 0 ? null : relationsList;
	}

	@Override
	public List<Relation> getIncomingRelationNodes(K from, String type) {
		logger.info("Inside getIncomingRelationNodes");

		List<Relation> relationsList = Lists.newArrayList();
		for (Entry<K, Versioned<Multimap<K, Long>>> e : from2toMap.entrySet()) {
			Multimap<K, Long> toMap = e.getValue().value();
			if (toMap.containsKey(from)) {
				List<Long> toList = (List<Long>) toMap.get(from);
				toList.stream().forEach(t -> relationsList.add(relationsMap.get(t).value()));
			}
		}

		if (relationsList.size() == 0) {
			logger.info("No incoming nodes");
			return null;
		}

		for (Relation relation : relationsList) {
			if (from2TypeMap.containsKey((K) relation.getFrom())) {
				Multimap<String, Long> typeMap = from2TypeMap.get((K) relation.getFrom()).value();
				if (!typeMap.containsEntry(type, relation.getId())) {
					relationsList.remove(relation);
				}
			} else {
				logger.error("Some catastrophe DEBUG HERE");
				relationsList.remove(relation);
			}
		}
		return relationsList.size() == 0 ? null : relationsList;
	}

	@Override
	public boolean areRelated(K from, K to) {
		logger.info("Inside areRelated");
		if (from2toMap.containsKey(from)) {
			Multimap<K, Long> fromMap = from2toMap.get(from).value();
			if (fromMap.containsKey(from)) {
				return true;
			}
		} else {
			logger.error("Catastrophic shit!! DEBUG");
			return false;
		}
		return false;
	}

	@Override
	public Relation getRelation(long relationId) {
		logger.info("Inside getRelation");
		if (relationsMap.containsKey(relationId)) {
			return relationsMap.get(relationId).value();
		}
		logger.error(String.format("Relation with id:% does not exist", relationId));
		return null;
	}

}
