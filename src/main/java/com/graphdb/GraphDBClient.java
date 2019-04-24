package com.graphdb;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Objects;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import com.graphdb.agent.ClusterAgent;
import com.graphdb.model.GraphModel;

import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;

public class GraphDBClient {

	public static void main(String... args) {

		String memberID = args[0];
		String memberPort = args[1];

		Objects.requireNonNull(memberID);
		Objects.requireNonNull(memberPort);

		Atomix atomixAgent = ClusterAgent.getDefaultAgent(memberID, memberPort, true);

//		AtomixBuilder atomixBuilder = Atomix.builder().withMemberId("client1").withAddress("localhost:8084")
//				.withMembershipProvider(BootstrapDiscoveryProvider.builder()
//						.withNodes(Node.builder().withId("member1").withAddress("localhost:8080").build(),
//								Node.builder().withId("member2").withAddress("localhost:8081").build(),
//								Node.builder().withId("member3").withAddress("localhost:8082").build(),
//								Node.builder().withId("member4").withAddress("localhost:8083").build())
//						.build());
//
//		Atomix atomixAgent = atomixBuilder.build();
		
		atomixAgent.start().join();

		GraphModel<String, Multimap<String, String>> graph = new GraphModel<>(atomixAgent, "multimap");
		ProxyProtocol protocol = MultiRaftProtocol.builder().withReadConsistency(ReadConsistency.LINEARIZABLE).build();
		graph.withProtocol(protocol);
		graph.buildAtomicMultiMap();

		Multimap<String, String> nodeData1 = HashMultimap.create();
		nodeData1.put("nodeId", "Node_1");
		graph.addNode("Node_1", nodeData1);

		Multimap<String, String> nodeData2 = HashMultimap.create();
		nodeData1.put("nodeId", "Node_2");
		graph.addNode("Node_2", nodeData2);

		Multimap<String, String> node = graph.getNode("Node_1");

		System.out.println(node.get("nodeId"));

		Multimap map = graph.getNode("Node_1");

		System.out.println(map.toString());
	}
}
