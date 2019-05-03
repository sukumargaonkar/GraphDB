package com.graphdb.model;

public class Relation<K, V> {

	private long id;
	private K from;
	private K to;
	private V value;
	private String type;
	private boolean biDirectional;

	public Relation(long id, K from, K to, V value, String type, boolean biDirectional) {
		this.id = id;
		this.from = from;
		this.to = to;
		this.value = value;
		this.type = type;
		this.biDirectional = biDirectional;
	}

	@Override
	public String toString() {
		return from.toString() + (isBiDirectional()? " <" : " ") + "-- (ID:" + id + " , Type:" + type + " , BiDirectional:" + isBiDirectional() + ") --> " + to.toString();
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

	public K getFrom() {
		return from;
	}

	public K getTo() {
		return to;
	}

	public String getType() {
		return type;
	}

	public boolean isBiDirectional() {
		return biDirectional;
	}
}
