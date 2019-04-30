package com.graphdb.model;

public class Relation<K, V> {

	private long id;
	private K from;
	private K to;
	private boolean biDirectional;
	private V value;

	public Relation(long id, K from, K to, V value, boolean biDirectional) {
		this.id = id;
		this.from = from;
		this.to = to;
		this.biDirectional = biDirectional;
		this.value = value;
	}

	public V getValue() {
		return value;
	}

	public void setValue(V value) {
		this.value = value;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public K getFrom() {
		return from;
	}

	public void setFrom(K from) {
		this.from = from;
	}

	public K getTo() {
		return to;
	}

	public void setTo(K to) {
		this.to = to;
	}

	public boolean isBiDirectional() {
		return biDirectional;
	}

	public void setBiDirectional(boolean biDirectional) {
		this.biDirectional = biDirectional;
	}

}
