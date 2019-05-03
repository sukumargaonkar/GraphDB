package GraphDB;

import java.util.Set;

import org.junit.Test;

import com.graphdb.agent.ClusterAgent;

import io.atomix.cluster.Member;
import io.atomix.core.Atomix;

/**
 * Unit test for simple ClusterInitializer.
 */
public class ClusterInitializerTest {

	@Test
	public void checkClusterConfig() {
		Atomix testAgent = ClusterAgent.getDefaultAgent("localhost", "8086");
		Set<Member> members = testAgent.getMembershipService().getMembers();
	}
}
