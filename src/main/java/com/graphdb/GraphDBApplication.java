package com.graphdb;

import org.apache.log4j.Logger;

import com.graphdb.agent.ClusterAgent;

import io.atomix.core.Atomix;
import io.atomix.core.map.DistributedMap;
import io.atomix.core.multimap.AtomicMultimapBuilder;

public class GraphDBApplication {

	private static final Logger logger = Logger.getLogger(GraphDBApplication.class);

	public static void main(String[] args) {

		String memberID = args[0];
		String memberPort = args[1];

		logger.info("Member ID:" + memberID + " memberPort:" + memberPort);

		Atomix atomix = ClusterAgent.getDefaultAgent(memberID, memberPort);

		System.out.println("Starting Cluster!");

		atomix.start().join();

		AtomicMultimapBuilder<String, String> multimapBuilder = atomix.<String, String>atomicMultimapBuilder("my-multimap");
		DistributedMap<String, String> map = atomix.<String, String>mapBuilder("graph-map").withCacheEnabled().build();

		map.put("foo", "Hello world!");

		String value = (String) map.get("foo");

		System.out.println(value);

		if (map.replace("foo", value, "Hello world again!")) {

		}

	}
}
