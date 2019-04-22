# GraphDB

A simple NoSQL Distributed Graph store. 

The project makes use of [Atomix](https://atomix.io/), a reactive Java framework for building fault tolerant distributed systems. It utilizes the Raft protocol for achieving consensus between its nodes and also provides several datastructures which are called primitives in its lingo for developers open to create a wide variety of applications.

The project is built using Java and to run you must have Apache Maven installed and added as a path/environment variable:

On cloning this repository execute run.bat (For Windows Only).

The file contains the following commands that will execute the application with 4 nodes.
```
start cmd.exe /k "mvn clean package"
timeout /t 10
start cmd.exe /k "java -jar .\target\graphdb.jar member1 8080"
start cmd.exe /k "java -jar .\target\graphdb.jar member2 8081"
start cmd.exe /k "java -jar .\target\graphdb.jar member3 8082"
start cmd.exe /k "java -jar .\target\graphdb.jar member4 8083"
```
