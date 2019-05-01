package com.graphdb.model;

import java.util.List;

import com.google.common.base.Optional;

import io.atomix.primitive.protocol.ProxyProtocol;

public interface Graph<K, V> {

	public void withProtocol(ProxyProtocol protocol);

	public void withCacheSize(int size);

	public void setReadOnly();

	public void buildAtomicMultiMap();

	public boolean addNode(K key, V value);

	public boolean removeNode(K key);

	public Optional<V> getNode(K key);

	public List<Long> addRelation(K from, K to, String type, V value, boolean biDirectional);

	public boolean removeRelation(long id);

	public boolean removeRelation(K from, K to, String type, boolean biDirectional);

	public Relation<K, V> getRelationById(long relationId);

	public String getRelationType(K from, K to);

	public long getNodeDegree(K from);

	public long getNodeDegree(long id);

	public List<Relation<K, V>> getOutgoingRelations(K from);

	public List<Relation<K, V>> getOutgoingRelations(long relationId);

	public List<Long> getOutgoingRelationNodes(K from);

	public List<Relation<K, V>> getIncomingRelations(K from);

	public List<Relation<K, V>> getIncomingRelations(long relationId);

	public List<Long> getIncomingRelationNodes(K from);

	public boolean areRelated(K from, K to);

}
