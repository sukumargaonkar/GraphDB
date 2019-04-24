package com.graphdb;

import java.util.Objects;

import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.graphdb.agent.ClusterAgent;
import com.graphdb.model.GraphModel;

import io.atomix.core.Atomix;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;

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
