package org.softwareheritage.graph.tinkerpop;

import com.martiansoftware.jsap.*;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.softwareheritage.graph.SwhBidirectionalGraph;
import org.webgraph.tinkerpop.WebgraphGremlinQueryExecutor;
import org.webgraph.tinkerpop.structure.WebGraphGraph;
import org.webgraph.tinkerpop.structure.provider.SimpleWebGraphPropertyProvider;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class Benchmark {

    private static final String EXAMPLE = "src/main/resources/example/example";
    private static final String PYTHON_3K = "src/main/resources/python3kcompress/graph";

    private final WebGraphGraph graph;
    private final WebgraphGremlinQueryExecutor e;
    private final long samples;
    private final int iters;

    public static void main(String[] args) throws IOException, JSAPException {
        SimpleJSAP jsap = new SimpleJSAP(Benchmark.class.getName(),
                "Server to load and query a compressed graph representation of Software Heritage archive.",
                new Parameter[]{
                        new FlaggedOption("graphPath", JSAP.STRING_PARSER, PYTHON_3K, JSAP.NOT_REQUIRED, 'g', "path",
                                "The basename of the compressed graph."),
                        new FlaggedOption("query", JSAP.STRING_PARSER, null, JSAP.REQUIRED, 'q', "query",
                                "The query to  profile."),
                        new FlaggedOption("iters", JSAP.INTEGER_PARSER, "10", JSAP.NOT_REQUIRED, 'i', "iters",
                                "The number of iterations on a single query."),
                        new FlaggedOption("samples", JSAP.INTEGER_PARSER, "10", JSAP.NOT_REQUIRED, 's', "samples",
                                "The number of samples picked for the query."),
                        new FlaggedOption("argument", JSAP.LONG_PARSER, "-1", JSAP.NOT_REQUIRED, 'a', "argument",
                                "If present, profiles the query with the argument, instead of doing iterations."),});

        JSAPResult config = jsap.parse(args);
        if (jsap.messagePrinted()) {
            System.exit(1);
        }

        String path = config.getString("graphPath");
        String query = config.getString("query");
        int iters = config.getInt("iters");
        int samples = config.getInt("samples");
        long argument = config.getLong("argument");

        System.out.println("Loading graph...");
        SwhBidirectionalGraph swhGraph = SwhBidirectionalGraph.loadLabelled(path);
        SimpleWebGraphPropertyProvider swh = SwhProperties.withEdgeLabels(swhGraph);
        WebGraphGraph graph = WebGraphGraph.open(swhGraph, swh, path);
        Benchmark benchmark = new Benchmark(graph, samples, iters);
        System.out.println("Done");

        benchmark.runQueryByName(query, argument);
    }

    public Benchmark(WebGraphGraph graph, long samples, int iters) {
        this.graph = graph;
        this.samples = samples;
        this.iters = iters;
        this.e = new WebgraphGremlinQueryExecutor(graph);
    }

    private void snapshotRevisionsWithBranches() {
        List<Long> startIds = randomVerticesWithLabel("SNP", samples);
        profileVertexQuery(startIds, Query::snapshotRevisionsWithBranches);
    }

    private void recursiveContentPathsWithPermissions() {
        List<Long> startIds = randomVerticesWithLabel("REV", samples);
        profileVertexQuery(startIds, Query::recursiveContentPathsWithPermissions);
    }

    private void earliestContainingCommit() {
        List<Long> startIds = randomVerticesWithLabel("CNT", samples);
        profileVertexQuery(startIds, Query::earliestContainingCommit);
    }

    private <S, E> void profileVertexQuery(List<Long> startIds, Function<Long, Function<GraphTraversalSource, GraphTraversal<S, E>>> query) {
        System.out.println("Profiling query for ids: " + startIds);
        long totalMs = 0;
        for (Long id : startIds) {
            System.out.println("Running query for id: " + id);
            long totalMsPerId = 0;
            for (int i = 0; i < iters; i++) {
                System.out.println(i + 1 + "/" + iters);
                TraversalMetrics metrics = profile(query.apply(id));
                long durationMs = metrics.getDuration(TimeUnit.MILLISECONDS);
                System.out.println("Finished in: " + durationMs + "ms\n");
                totalMsPerId += durationMs;
            }
            long average = totalMsPerId / iters;
            totalMs += average;
            System.out.printf("Average for id: %d - %dms%n%n", id, average);
        }
        System.out.printf("Average: - %dms%n%n", totalMs / startIds.size());
    }

    private void runQueryByName(String name, long arg) {
        if (arg != -1) {
            System.out.println("Argument provided, running query once for id: " + arg);
        }
        if (name.equals("earliestContainingCommit")) {
            if (arg == -1) {
                earliestContainingCommit();
            } else {
                profile(Query.earliestContainingCommit(arg));
            }
        } else if (name.equals("recursiveContentPathsWithPermissions")) {
            if (arg == -1) {
                recursiveContentPathsWithPermissions();
            } else {
                profile(Query.recursiveContentPathsWithPermissions(arg));
            }
        } else if (name.equals("snapshotRevisionsWithBranches")) {
            if (arg == -1) {
                snapshotRevisionsWithBranches();
            } else {
                profile(Query.snapshotRevisionsWithBranches(arg));
            }
        } else {
            System.out.println("Unknown query name: " + name);
        }
    }

    private <S, E> TraversalMetrics profile(Function<GraphTraversalSource, GraphTraversal<S, E>> query) {
        return e.profile(query);
    }

    private List<Long> randomVerticesWithLabel(String label, long count) {
        return e.get(g -> g.V().hasLabel(label)
                           .order().by(Order.shuffle)
                           .limit(count)
                           .id().map(id -> (long) id.get()));
    }

}
