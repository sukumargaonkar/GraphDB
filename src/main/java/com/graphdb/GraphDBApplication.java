package com.graphdb;

import java.util.Objects;

import org.apache.log4j.Logger;

import com.graphdb.agent.ClusterAgent;

import io.atomix.core.Atomix;

public class GraphDBApplication {

	private static final Logger logger = Logger.getLogger(GraphDBApplication.class);

	public static void main(String[] args) {

		String memberID = args[0];
		String memberPort = args[1];

		Objects.requireNonNull(memberID);
		Objects.requireNonNull(memberPort);

		logger.info("Member ID:" + memberID + " memberPort:" + memberPort);

		Atomix clusterAgent = ClusterAgent.getDefaultAgent(memberID, memberPort);

		logger.info("Starting Cluster!");

		clusterAgent.start().join();

	}
}
