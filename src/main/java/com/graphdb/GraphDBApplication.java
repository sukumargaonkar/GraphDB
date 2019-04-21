package com.graphdb;

import java.util.Collection;
import java.util.Objects;

import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.graphdb.agent.ClusterAgent;
import com.graphdb.agent.JsonAgent;
import com.graphdb.model.GraphModel;

import io.atomix.core.Atomix;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.utils.time.Versioned;

public class GraphDBApplication {

	private static final Logger logger = Logger.getLogger(GraphDBApplication.class);

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {

		String memberID = args[0];
		String memberPort = args[1];

		Objects.requireNonNull(memberID);
		Objects.requireNonNull(memberPort);

		logger.info("Member ID:" + memberID + " memberPort:" + memberPort);

		Atomix clusterAgent = ClusterAgent.getDefaultAgent(memberID, memberPort);

		logger.info("Starting Cluster!");

		clusterAgent.start().join();

		GraphModel<String, String> graph = new GraphModel<String, String>(clusterAgent, "multimap", false);
		ProxyProtocol protocol = MultiRaftProtocol.builder().withReadConsistency(ReadConsistency.LINEARIZABLE).build();
		graph.withProtocol(protocol);
		graph.buildAtomicMultiMap();

		Multimap<String, String> multimap = HashMultimap.create();
		multimap.put("hi", "hello");
		multimap.put("hi", "kk");

		// Json builder
		JsonAgent<String> jsonAgent = new JsonAgent<String>();
		String jsonGraph = jsonAgent.toJson(multimap.asMap());

		System.out.println(jsonGraph);

		graph.addNode("123", jsonGraph);

		Versioned<Collection<String>> node = (Versioned<Collection<String>>) graph.getNode("123");

		System.out.println(node.value());
	}
}
