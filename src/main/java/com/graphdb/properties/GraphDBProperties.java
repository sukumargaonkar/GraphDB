package com.graphdb.properties;

import java.util.Properties;

public class GraphDBProperties {

	private static Properties props;

	public static Properties getGraphDBProperties() {
		props = new Properties();
		props.setProperty("memberNames", "member1,member2,member3,member4");
		props.setProperty("managementData", System.getProperty("user.dir") + "/clusterDir/clusterMeta/");
		props.setProperty("propertyData", System.getProperty("user.dir") + "/clusterDir/data/");
		return props;
	}
}
