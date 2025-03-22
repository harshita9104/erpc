# neo4j

This directory contains neo4j configurations and neo4j can be launched with
the docker-compose.yml file.

## Launching neo4j

Inside of this directory, run: `docker-compose up`.

It should create the new directories: `data`, `import`, `logs` and `plugins`.

It's possible that you need to change the directory permissions to UID=7474.

## Launching cypher-shell

`cypher-shell` can be launched from a running docker container:
`docker exec -it neo4j-neo4j-1 cypher-shell`

## Exporting a full neo4j database into CSV files

Inside a `cypher-shell` run:

```bash
CALL apoc.export.csv.all("neo4j.csv", {bulkImport:true});
```

It'll create separate nodes and relationships files that can be imported in a
neo4j database.

The current format of the exported CSV is:

`*.nodes.*.csv`:

```bash
:ID,fingerprint,:LABEL
0,$bc61636546ed21a49fec0a532064db9538f7c430,Relay
```

`*.relationships*.csv`:

```bash
:START_ID,:END_ID,:TYPE,message,timestamp
2257,3021,CIRCUIT_SUCCESS,Success,1690450019
```

## Importing a neo4j CSV files into neo4j

Using `neo4j-admin` from a running docker container, run:

```bash
docker exec -it neo4j_neo4j_1 neo4j-admin database import full --overwrite-destination --nodes import/neo4j.nodes.Relay.csv --relationships import/eo4j.relationships.CIRCUIT_SUCCESS.csv,import/neo4j.relationships.CIRCUIT_FAILURE.csv
```

## Example analysis queries

In cypher-shell, count the total number of circuits:

```bash
MATCH (n)-[r]->() RETURN COUNT(r);
```

Count the total number of successful circuits:

```bash
MATCH p=()-[r:CIRCUIT_SUCCESS]->() RETURN COUNT(r);
```

## Possible partition detection algorithms

### Using `gds`` plugin

Some of the neo4j's [Community detection](https://neo4j.com/docs/graph-data-science/current/algorithms/community/) algorithms can be used:

- [Weakly Connected Components](https://neo4j.com/docs/graph-data-science/current/algorithms/wcc/)
- [Modularity Optimization](https://neo4j.com/docs/graph-data-science/current/algorithms/modularity-optimization/)
- [Strongly Connected Components](https://neo4j.com/docs/graph-data-science/current/algorithms/strongly-connected-components/)
- [Approximate Maximum k-cut](https://neo4j.com/docs/graph-data-science/current/algorithms/alpha/approx-max-k-cut/)


In cypher-shell, create a graph projection with the connected relays:

```bash
CALL gds.graph.project('graph', 'Relay', 'CIRCUIT_SUCCESS');
```

List [Strongly Connected Components (SCC)](https://neo4j.com/docs/graph-data-science/current/algorithms/strongly-connected-components/)
and their relay fingeprints:

```bash
CALL gds.alpha.scc.stream('graph', {})
YIELD nodeId, componentId
RETURN gds.util.asNode(nodeId).fingerprint, componentId
ORDER BY componentId ASC;
```

List SCC and their size:

```bash
MATCH (u:Relay)
RETURN u.componentId AS Component, count(*) AS ComponentSize
ORDER BY ComponentSize DESC
```

In `queries`` directory there're some community detection cypher shell queries
and bash scripts to run with neo4j docker container running.