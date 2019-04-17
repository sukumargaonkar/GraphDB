package GraphDB;

import io.atomix.cluster.Member;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.utils.net.Address;
import org.apache.log4j.Logger;

import java.io.File;

public class ClusterInitializer
{
//    static final Logger logger = Logger.getLogger(ClusterInitializer.class);

    public static void main( String[] args )
    {
//        Atomix atomix = Atomix.builder("configFiles/cluster.conf").build();

        String memberID = args[0];
        String memberPort = args[1];
//        logger.info("Starting Member: " + memberID);
        Atomix atomix = Atomix.builder()
                .withMemberId(memberID)
                .withAddress(new Address("localhost", Integer.parseInt(memberPort)))
                .withMembershipProvider(BootstrapDiscoveryProvider.builder()
                        .withNodes(
                                Member.builder()
                                        .withId("member1")
                                        .withAddress(new Address("localhost", 8800))
                                        .build(),
                                Member.builder()
                                        .withId("member2")
                                        .withAddress(new Address("localhost", 8801))
                                        .build(),
                                Member.builder()
                                        .withId("member3")
                                        .withAddress(new Address("localhost", 8802))
                                        .build())
                        .build())
                .withManagementGroup(RaftPartitionGroup.builder("system")
                        .withDataDirectory(new File(System.getProperty("user.dir") + "/clusterDirs/" + memberID))
                        .withNumPartitions(1)
                        .withMembers("member1", "member2", "member3")
                        .build())
                .withPartitionGroups(RaftPartitionGroup.builder("raft")
                        .withDataDirectory(new File(System.getProperty("user.dir") + "/clusterDirs/" + memberID))
                        .withPartitionSize(3)
                        .withNumPartitions(3)
                        .withMembers("member1", "member2", "member3")
                        .build())
                .build();

        System.out.println( "Starting Cluster!" );
        System.out.println(atomix.start().join());

        System.out.println( "Hello World!" );
    }
}
