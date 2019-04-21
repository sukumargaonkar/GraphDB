package com.graphdb.model;

import java.util.Collection;

import io.atomix.core.Atomix;
import io.atomix.core.multimap.AsyncAtomicMultimap;
import io.atomix.core.multimap.AtomicMultimap;
import io.atomix.core.multimap.AtomicMultimapBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;

public class GraphModel<K, V> {

	private AtomicMultimapBuilder<K, V> atomicMultiMapBuilder;
	private AtomicMultimap<K, V> atomicMultiMap;
	private AsyncAtomicMultimap<K, V> asyncAtomicMultiMap;

	private boolean async;

	public GraphModel(Atomix atomix, String name, boolean async) {
		atomicMultiMapBuilder = atomix.<K, V>atomicMultimapBuilder(name).withCacheEnabled();
		this.async = async;
	}

	public void withProtocol(ProxyProtocol protocol) {
		atomicMultiMapBuilder.withProtocol(protocol);
	}

	public void setReadOnly() {
		atomicMultiMapBuilder.withReadOnly();
	}

	public void buildAtomicMultiMap() {
		atomicMultiMap = atomicMultiMapBuilder.build();
	}

	public void buildAsyncAtomicMultiMap() {
		asyncAtomicMultiMap = atomicMultiMapBuilder.build().async();
	}

	public void addNode(K key, V value) {
		if (async) {
			asyncAtomicMultiMap.put(key, value);
		} else {
			atomicMultiMap.put(key, value);
		}
	}

	public void addAllNode(K key, Collection<V> values) {
		if (async) {
			asyncAtomicMultiMap.putAll(key, values);
		} else {
			atomicMultiMap.putAll(key, values);
		}
	}

}
