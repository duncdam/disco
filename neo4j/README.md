# Neo4J 

This repo describes the resources needed to create and run a Neo4J instance as a Docker container. 

It also includes the APOC library as a plugin. This library contains a lot of useful functions and procedures.

You can read more about APOC [here:](https://neo4j.com/labs/apoc/) 

## Getting started

Before building the container, you will need to export the following Environment Variables:

* NEO4J_USER
* NEO4J_PASS


### Building the container

In the root directory of the repo, run this command:
`docker-compose build`

### Running the container

`docker-compose up`

### The Graph Browser

Once the graph container is up and running, you can use a browser to open:
`http://localhost:7474/browser/`


#### Run a simple query in the Graph Browser
`match(n) return count(n);`

An empty database will return 0 results.

### Cypher-shell
To open a cypher-shell, follows these steps:

1. Create a new terminal session in the repo directory while the container is running.
2. Run `docker ps` and note the **Container Id**.
3. Run `docker exec -it $CONTAINER_ID bash` to open a bash prompt inside the running container.
4. Inside the running container bash prompt, run `cypher-shell -u $NEO4J_USER -p $NEO4J_PASS` to open the cypher-shell.
5. Run a simple query in the cypher-shell to verify operation: `match(n) return count(n);`
6. To exit the cypher-shell, run `:exit`.
7. To exit the running container bash prompt, run `exit`.


Here is a example walk-through *(please note that NEO4J user/pass has been redacted below)*:
```bash
wintermute::cda-search-graph-neo4j$ docker ps
CONTAINER ID        IMAGE                          COMMAND                  CREATED             STATUS              PORTS                                                      NAMES
9adb3125d901        cda-search-graph-neo4j_neo4j   "/sbin/tini -g -- /d…"   7 minutes ago       Up 7 minutes        0.0.0.0:7474->7474/tcp, 7473/tcp, 0.0.0.0:7687->7687/tcp   cda-search-graph-neo4j_neo4j_1
wintermute::cda-search-graph-neo4j$ docker exec -it 9adb3125d901 bash
bash-4.4# cypher-shell -u $NEO4J_USER -p $NEO4J_PASS
Connected to Neo4j 3.5.3 at bolt://localhost:7687 as user neo4j.
Type :help for a list of available commands or :exit to exit the shell.
Note that Cypher queries must end with a semicolon.
neo4j> match(n) return count(n);
+----------+
| count(n) |
+----------+
| 0        |
+----------+

1 row available after 1 ms, consumed after another 0 ms
neo4j> :exit

Bye!
bash-4.4# exit
exit
```