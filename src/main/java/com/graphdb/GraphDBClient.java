package com.graphdb;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.graphdb.agent.JsonAgent;
import com.graphdb.model.GraphModel;

import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;

public class GraphDBClient {

	private final static Logger logger = Logger.getLogger(GraphDBClient.class);

	public static void main(String... args) {

		logger.info("Client starting....");

		String clientAddress = args[0];
		
		if (Objects.isNull(clientAddress)) {
			clientAddress = "localhost:8084";
		}

		AtomixBuilder atomixBuilder = Atomix.builder().withMemberId("client1").withAddress(clientAddress)
				.withMembershipProvider(BootstrapDiscoveryProvider.builder()
						.withNodes(Node.builder().withId("member1").withAddress("localhost:8080").build(),
								Node.builder().withId("member2").withAddress("localhost:8081").build(),
								Node.builder().withId("member3").withAddress("localhost:8082").build(),
								Node.builder().withId("member4").withAddress("localhost:8083").build())
						.build());

		Atomix atomixAgent = atomixBuilder.build();

		atomixAgent.start().join();

		logger.info("Client started....");

		GraphModel<String, String> graph = new GraphModel<>(atomixAgent, "multimap");
		ProxyProtocol protocol = MultiRaftProtocol.builder().withReadConsistency(ReadConsistency.LINEARIZABLE).build();
		graph.withProtocol(protocol);
		graph.buildAtomicMultiMap();

		JsonAgent<Map<String, Collection<String>>> jsonAgent = new JsonAgent<>();

		Multimap<String, String> nodeData1 = HashMultimap.create();
		nodeData1.put("1", "abc");
		nodeData1.put("1", "fool");
		nodeData1.put("1", "def");

		String json = jsonAgent.toJson(nodeData1.asMap());
		graph.addNode("node1", json);

		String node = graph.getNode("node1");
		System.out.println(node);
	}
}
