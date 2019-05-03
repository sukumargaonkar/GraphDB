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

	public Long addRelation(K from, K to, String type, V value, boolean biDirectional);

	public boolean removeRelation(long id);

	public boolean removeRelation(K from, K to, String type);

	public Relation getRelations(long relationId);

	public List<Relation> getRelations(K from, K to);

	public List<String> getRelationType(K from, K to);

	public long getNodeOutDegree(K from);

	public List<Relation> getOutgoingRelations(K from);

	public List<Relation> getOutgoingRelations(K from, String type);

	public List<Relation> getIncomingRelations(K from);

	public List<Relation> getIncomingRelations(K from, String type);

	public boolean areRelated(K from, K to);

	public List<K> search(K from, K to);
}
