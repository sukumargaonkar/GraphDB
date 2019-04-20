package GraphDB;

import io.atomix.cluster.Member;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.utils.net.Address;
import org.apache.log4j.Logger;

import java.io.File;

public class ClusterInitializer {
//    static final Logger logger = Logger.getLogger(ClusterInitializer.class);

	public static void main(String[] args) {
//        Atomix atomix = Atomix.builder("configFiles/cluster.conf").build();

		String[] members = {"member1", "member2", "member3", "member4"};
		Node[] nodes = new Node[members.length];
		for (int i = 0; i < members.length; i++) {
			nodes[i] = Member.builder()
						.withId(members[i])
						.withAddress(new Address("localhost", 8800 + i))
						.build();
		}

		String memberID = args[0];
		String memberPort = args[1];

		Atomix atomix = Atomix.builder()
				.withMemberId(memberID)
				.withAddress(new Address("localhost", Integer.parseInt(memberPort)))
				.withMembershipProvider(BootstrapDiscoveryProvider.builder()
						.withNodes(nodes)
						.build())
				.withManagementGroup(RaftPartitionGroup.builder("system")
						.withDataDirectory(new File(System.getProperty("user.dir") + "/ClusterDirs/ClusterMetaData/" + memberID))
						.withNumPartitions(1)
						.withMembers(members)
						.build())
				.withPartitionGroups(RaftPartitionGroup.builder("raft")
						.withDataDirectory(new File(System.getProperty("user.dir") + "/ClusterDirs/Data/" + memberID))
						.withPartitionSize(2)
						.withNumPartitions(10)
						.withMembers(members)
						.build())
				.build();

		System.out.println("Starting Cluster!");
		System.out.println(atomix.start().join());

		System.out.println("Hello World!");
	}
}
