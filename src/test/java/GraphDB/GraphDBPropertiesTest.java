package GraphDB;

import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Test;

import com.graphdb.properties.GraphDBProperties;

public class GraphDBPropertiesTest {

	@Test
	public void testProperties() {
		Properties props = GraphDBProperties.getGraphDBProperties();
		assertTrue(props.getProperty("memberNames").contentEquals("member1,member2,member3,member4"));
		assertTrue(props.getProperty("managementData")
				.contentEquals(System.getProperty("user.dir") + "/clusterDir/clusterMeta/"));
		assertTrue(
				props.getProperty("propertyData").contentEquals(System.getProperty("user.dir") + "/clusterDir/data/"));
	}
}
