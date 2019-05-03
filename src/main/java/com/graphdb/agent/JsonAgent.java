package com.graphdb.agent;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JsonAgent<T> {

	private static Gson gson;

	public JsonAgent() {
		if (gson == null)
			gson = new GsonBuilder().setPrettyPrinting().serializeNulls().create();
	}

	public String toJson(Object json) {
		return gson.toJson(json);
	}

	public T fromJson(String json, Class<T> classOfT) {
		return gson.fromJson(json, classOfT);
	}

}
