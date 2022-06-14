# [webgraph-tinkerpop](https://github.com/SoftwareHeritage/webgraph-tinkerpop) implementation for swh-graph


Showcases the usage of [webgraph-tinkerpop](https://github.com/SoftwareHeritage/webgraph-tinkerpop) for the Software Heritage graph.

Also includes benchmarking code.
________

## Build

The code depends on [webgraph-tinkerpop](https://github.com/SoftwareHeritage/webgraph-tinkerpop) and [swh-graph](https://forge.softwareheritage.org/source/swh-graph/) installed into the local Maven repository.

Installing `webgraph-tinkerpop`:

```shell
git clone https://github.com/SoftwareHeritage/webgraph-tinkerpop.git
cd webgraph-tinkerpop
mvn install
```

Installing `swh-graph` version `0.6.1`:
```shell
git clone https://forge.softwareheritage.org/source/swh-graph.git
cd swh-graph/java
mvn package -Dmaven.javadoc.skip=true -Dmaven.test.skip=true
mvn install:install-file -Dfile=target/swh-graph-0.6.1.jar -DgroupId=org.softwareheritage.graph -DartifactId=swh-graph -Dversion=0.6.1 -Dpackaging=jar
```

To build the library:
```shell
mvn compile assembly:single
```
________________


## Properties
In [SwhProperties.java](https://github.com/SoftwareHeritage/swh-graph-tinkerpop/blob/master/src/main/java/org/softwareheritage/graph/tinkerpop/SwhProperties.java) you can find all regiestered properties.
These include:

| Key  | Type | Description |
| ---- | ---- | ----------- |
| -- (Label) | String | Node type |
| `author_timestamp`  | Long | Author timestamp from `author_timestamp.bin` file |
| `swhid`  | String | The SWHID of the node |
| `message`  | String | The message for the node |
| `__arc_label_property__`  | DirEntry[] | The DirEntry for the edge |
| `dir_entry_str`  | DirEntryString[] | The DirEntry for the edge with the filename converted to String |

________________

## Server

Running Gremlin queries on an SWH graph:

```shell
java -cp target/*.jar org.softwareheritage.graph.tinkerpop.Server <graph_path> <query> [--profile]
```
* `graph_path` - path to the graph folder
* `query` - a Gremlin query to execute on the graph
* `--profile` - instead of query results outputs profiling results

## Benchmarker

To run the benchmarker:

```shell
java -cp target/*.jar org.softwareheritage.graph.tinkerpop.Benchmark <options>
```
Available options:
* `--path <graphPath>` - path to the graph folder. Defaults to [example graph](https://github.com/SoftwareHeritage/swh-graph-tinkerpop/tree/master/src/main/resources/example)
* `--query <query>` - the query key: `earliestContainingRevision | originOfRevision | recursiveContentPathsWithPermissions | snapshotRevisionsWithBranches`
* `--samples <samples>` - the number of samples to run the query on
* `--iters <iters>` - the number of iterations per sample
* `--argument <argument>` - if present, profiles the query with the argument, instead of doing iterations
* * `--print` - if present, prints the query outputs

Example:
```shell
java -cp target/*.jar org.softwareheritage.graph.tinkerpop.Benchmark --path src/main/resources/example/example --query recursiveContentPathsWithPermissions --iters 3 --samples 100
```
