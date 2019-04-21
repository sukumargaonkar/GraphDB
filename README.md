# GraphDB

A simple NoSQL Distributed Graph store. 

The project makes use of [Atomix](https://atomix.io/), a reactive Java framework for building fault tolerant distributed systems. It utilizes the Raft protocol for achieving consensus between its nodes and also provides several datastructures which are called primitives in its lingo for developers open to create a wide variety of applications.
The project is built using Java and to build it using Apache Maven use:
```
mvn clean install
```
