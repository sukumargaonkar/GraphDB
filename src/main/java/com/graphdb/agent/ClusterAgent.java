package com.graphdb.agent;

import java.io.File;

import org.apache.log4j.Logger;

import io.atomix.cluster.Member;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.utils.net.Address;

public class ClusterAgent {
	private final static Logger logger = Logger.getLogger(ClusterAgent.class);

	public static Atomix getDefaultAgent(String member, String portID) {

		logger.info("Creating Atomix configuration");

		String[] members = { "member1", "member2", "member3", "member4" };
		Node[] nodes = new Node[members.length];
		for (int i = 0; i < members.length; i++) {
			nodes[i] = Member.builder().withId(members[i]).withAddress(new Address("localhost", 8800 + i)).build();
		}

		Atomix atomix = Atomix.builder().withMemberId(member)
				.withAddress(new Address("localhost", Integer.parseInt(portID)))
				.withMembershipProvider(BootstrapDiscoveryProvider.builder().withNodes(nodes).build())
				.withManagementGroup(RaftPartitionGroup.builder("system")
						.withDataDirectory(
								new File(System.getProperty("user.dir") + "/clusterDir/clusterMeta/" + member))
						.withNumPartitions(1).withMembers(members).build())
				.withPartitionGroups(RaftPartitionGroup.builder("raft")
						.withDataDirectory(new File(System.getProperty("user.dir") + "/clusterDir/data/" + member))
						.withPartitionSize(2).withNumPartitions(10).withMembers(members).build())
				.build();
		return atomix;
	}
}
