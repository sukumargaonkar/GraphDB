package com.graphdb.model;

import java.util.*;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
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
	private AtomicMapBuilder<K, Map<String, Collection<Long>>> from2TypeBuilder;
	private AtomicMapBuilder<K, Map<K, Collection<Long>>> from2ToBuilder;
	private AtomicMap<K, V> nodes;
	public AtomicMap<Long, Relation> relationsMap;
	public AtomicMap<K, Map<String, Collection<Long>>> from2TypeMap;
	private AtomicMap<K, Map<K, Collection<Long>>> from2ToMap;

	private static AtomicIdGenerator relationsIdGenerator;

	public GraphModelImpl(Atomix atomix, String name) {
		nodesMapBuilder = atomix.<K, V>atomicMapBuilder(name + "_nodes").withCacheEnabled();
		relationsMapBuilder = atomix.<Long, Relation>atomicMapBuilder(name + "_relations").withCacheEnabled();
		from2ToBuilder = atomix.<K, Map<K, Collection<Long>>>atomicMapBuilder(name + "_from2To").withCacheEnabled();
		from2TypeBuilder = atomix.<K, Map<String, Collection<Long>>>atomicMapBuilder(name + "_from2Type")
				.withCacheEnabled();
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
		from2ToMap = from2ToBuilder.build();
		from2TypeMap = from2TypeBuilder.build();
	}

	private long generateId() {
		return relationsIdGenerator.nextId();
	}

	public boolean addNode(K key, V value) {
		nodes.put(key, value);
		return true;
	}

	public Long addRelation(K from, K to, String type, V value, boolean biDirectional) {

		logger.info("Inside addRelation");

		if (!nodes.containsKey(from) || !nodes.containsKey(to)) {
			return null;
		}

//		Create Relation Object
		Relation<K, V> relation = new Relation<>(generateId(), from, to, value, type, biDirectional);
		relationsMap.put(relation.getId(), relation);

//		Add Index to the Relation Object
		if (!from2ToMap.containsKey(from)) {
			Map<K, Collection<Long>> mapHolder = new HashMap<>();
			mapHolder.put(to, Arrays.asList(relation.getId()));
			from2ToMap.put(from, mapHolder);
		} else {
			from2ToMap.get(from).value().get(to).add(relation.getId());
		}

		if (!from2TypeMap.containsKey(from)) {
			Map<String, Collection<Long>> mapHolder = new HashMap<>();
			mapHolder.put(type, Arrays.asList(relation.getId()));
			from2TypeMap.put(from, mapHolder);
		} else {
			from2TypeMap.get(from).value().get(type).add(relation.getId());
		}

		if (biDirectional) {
			if (!from2ToMap.containsKey(to)) {
				Map<K, Collection<Long>> mapHolder = new HashMap<>();
				mapHolder.put(from, Arrays.asList(relation.getId()));
				from2ToMap.put(to, mapHolder);
			} else {
				from2ToMap.get(to).value().get(from).add(relation.getId());
			}

			if (!from2TypeMap.containsKey(to)) {
				Map<String, Collection<Long>> mapHolder = new HashMap<>();
				mapHolder.put(type, Arrays.asList(relation.getId()));
				from2TypeMap.put(to, mapHolder);
			} else {
				from2TypeMap.get(to).value().get(type).add(relation.getId());
			}
		}

		return relation.getId();
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
		if (from2ToMap.containsKey(key)) {
			from2ToMap.remove(key);
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
			if (from2ToMap.containsKey((K) relation.getFrom())) {
				from2ToMap.remove((K) relation.getFrom());
			}
			if (from2TypeMap.containsKey((K) relation.getFrom())) {
				from2TypeMap.remove((K) relation.getFrom());
			}

			if (relation.isBiDirectional()) {
				if (from2ToMap.containsKey((K) relation.getTo())) {
					from2ToMap.remove((K) relation.getTo());
				}
				if (from2TypeMap.containsKey((K) relation.getTo())) {
					from2TypeMap.remove((K) relation.getTo());
				}
			}

			relationsMap.remove(id);
			return true;
		}
		logger.error(String.format("Relation id:% not found", id));
		return false;
	}

	@Override
	public boolean removeRelation(K from, K to, String type) {
		logger.info("Inside remove relation by from,to,type");
		boolean result = true;

		List<Long> relsToDelete = new ArrayList<>();
		if (from2ToMap.containsKey(from) & from2ToMap.get(from).value().containsKey(to)) {
			for (Long relId : from2ToMap.get(from).value().get(to)) {
				if (relationsMap.get(relId).value().getType().equals(type)) {
					relsToDelete.add(relId);
				}
			}
		}

		for (Long relID : relsToDelete) {
//			Indicates if all the nodes were deleted or not.
			result &= removeRelation(relID);
		}

		return result;
	}

	@Override
	public Relation getRelations(long relationId) {
		logger.info("Inside getRelations");
		if (relationsMap.containsKey(relationId)) {
			return relationsMap.get(relationId).value();
		}
		logger.error(String.format("Relation with id:% does not exist", relationId));
		return null;
	}

	@Override
	public List<Relation> getRelations(K from, K to) {
		logger.info("Inside getRelations(from,to)");
		List<Relation> relations = Lists.newArrayList();
		if (from2ToMap.containsKey(from) & from2ToMap.get(from).value().containsKey(to)) {
			for (Long relId : from2ToMap.get(from).value().get(to)) {
				relations.add(relationsMap.get(relId).value());
			}
		}
		return relations;
	}

	@Override
	public List<String> getRelationType(K from, K to) {
		logger.info("Inside getRelationType");
		List<String> relationTypes = Lists.newArrayList();
		if (from2ToMap.containsKey(from) & from2ToMap.get(from).value().containsKey(to)) {
			for (Long relId : from2ToMap.get(from).value().get(to)) {
				relationTypes.add(relationsMap.get(relId).value().getType());
			}
		}
		return relationTypes;
	}

	/*
	 * calculate outdegree given a node
	 */
	@Override
	public long getNodeOutDegree(K key) {
		logger.info("Inside getNodeOutDegree");
		long outDegree = 0;
		if (from2ToMap.containsKey(key)) {
			for (Entry<K, Collection<Long>> entry : from2ToMap.get(key).value().entrySet()) {
				outDegree += entry.getValue().size();
			}
			return outDegree;
		}
		return -1;
	}

	@Override
	public List<Relation> getOutgoingRelations(K from) {
		logger.info("Inside getOutgoingRelations");
		List<Relation> relations = Lists.newArrayList();
		if (from2ToMap.containsKey(from)) {
			Map<K, Collection<Long>> fromMap = from2ToMap.get(from).value();
			for (Entry<K, Collection<Long>> entry : fromMap.entrySet()) {
				for (Long relId : entry.getValue()) {
					relations.add(relationsMap.get(relId).value());
				}
			}
			return relations;
		}
		return null;
	}

	@Override
	public List<Relation> getOutgoingRelations(K from, String type) {
		logger.info("Inside getOutgoingRelationNodes");
		List<Relation> relationsList = Lists.newArrayList();
		if (from2ToMap.containsKey(from)) {
			Map<String, Collection<Long>> fromMap = from2TypeMap.get(from).value();
			if (fromMap.containsKey(type)) {
				for (Long relId : fromMap.get(type)) {
					relationsList.add(relationsMap.get(relId).value());
				}
				return relationsList;
			}
		}
		return null;
	}

	@Override
	public List<Relation> getIncomingRelations(K to) {
		logger.info("Inside getIncomingRelations");
		List<Relation> relationsList = Lists.newArrayList();
		for (Entry<K, Versioned<Map<K, Collection<Long>>>> entry : from2ToMap.entrySet()) {
			Map<K, Collection<Long>> fromMap = entry.getValue().value();
			if (fromMap.containsKey(to)) {
				List<Long> toList = (List<Long>) fromMap.get(to);
				toList.forEach(relId -> relationsList.add(relationsMap.get(relId).value()));
			}
		}
		return relationsList.size() == 0 ? null : relationsList;
	}

	@Override
	public List<Relation> getIncomingRelations(K to, String type) {
		logger.info("Inside getIncomingRelations");

		List<Relation> relationsList = Lists.newArrayList();
		for (Entry<K, Versioned<Map<K, Collection<Long>>>> entry : from2ToMap.entrySet()) {
			Map<K, Collection<Long>> toMap = entry.getValue().value();
			if (toMap.containsKey(to)) {
				for (Long relId : toMap.get(to)) {
					if (relationsMap.get(relId).value().getType().equals(type)) {
						relationsList.add(relationsMap.get(relId).value());
					}
				}
			}
		}

		return relationsList;
	}

	@Override
	public boolean areRelated(K from, K to) {
		logger.info("Inside areRelated");
		if (from2ToMap.containsKey(from)) {
			Map<K, Collection<Long>> fromMap = from2ToMap.get(from).value();
			if (fromMap.containsKey(to)) {
				return true;
			}
		} else {
			logger.error("Node " + from + " not present in the Graph");
			return false;
		}
		return false;
	}
	
	/*
	 * Return shortest path from a node to another
	 * @return: A List of node keys in order
	 */
	@Override
	public List<K> search(K from, K to) {
		return null;
	}

}
