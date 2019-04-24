package com.graphdb.agent;

import java.io.File;
import java.util.Properties;

import io.atomix.core.AtomixBuilder;
import org.apache.log4j.Logger;

import com.graphdb.properties.GraphDBProperties;

import io.atomix.cluster.Member;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.utils.net.Address;

public class ClusterAgent {

	private final static Logger logger = Logger.getLogger(ClusterAgent.class);
	private final static String MANAGEMENT_PARTITION_NAME = "system";
	private final static String PARTITION_GROUP_NAME = "raft";
	private final static String HOST = "localhost";
	private final static int PORT = 8800;

	public static Atomix getDefaultAgent(String member, String portID, boolean isClient) {

		logger.info("Creating Atomix configuration");

		Properties clusterProps = GraphDBProperties.getGraphDBProperties();
		String[] members = clusterProps.getProperty("memberNames").split(",");

		Node[] nodes = new Node[members.length];
		for (int i = 0; i < members.length; i++) {
			nodes[i] = Member.builder().withId(members[i]).withAddress(new Address(HOST, PORT + i)).build();
		}

		AtomixBuilder atomixBuilder = Atomix.builder().withMemberId(member).withAddress(new Address(HOST, Integer.parseInt(portID)))
				.withMembershipProvider(BootstrapDiscoveryProvider.builder().withNodes(nodes).build());

		if(!isClient) {
			atomixBuilder.withManagementGroup(RaftPartitionGroup.builder(MANAGEMENT_PARTITION_NAME)
					.withDataDirectory(new File(clusterProps.getProperty("managementData") + member))
					.withNumPartitions(1).withMembers(members).build())
					.withPartitionGroups(RaftPartitionGroup.builder(PARTITION_GROUP_NAME)
							.withDataDirectory(new File(clusterProps.getProperty("propertyData") + member))
							.withPartitionSize(2).withNumPartitions(10).withMembers(members).build());
		}
		return atomixBuilder.build();
	}
}
