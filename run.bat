start cmd.exe /k "mvn clean package"
timeout /t 10
start cmd.exe /k "java -jar .\target\graphdb.jar member1 8080"
start cmd.exe /k "java -jar .\target\graphdb.jar member2 8081"
start cmd.exe /k "java -jar .\target\graphdb.jar member3 8082"
start cmd.exe /k "java -jar .\target\graphdb.jar member4 8083"