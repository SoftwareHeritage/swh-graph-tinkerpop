package org.softwareheritage.graph.tinkerpop;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.longs.LongLongImmutablePair;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.softwareheritage.graph.SwhBidirectionalGraph;
import org.webgraph.tinkerpop.WebgraphGremlinQueryExecutor;
import org.webgraph.tinkerpop.structure.WebGraphGraph;
import org.webgraph.tinkerpop.structure.provider.SimpleWebGraphPropertyProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

    private <S, E> void profileVertexQuery(List<Long> startIds, Function<Long, Function<GraphTraversalSource, GraphTraversal<S, E>>> query) {
        profileVertexQuery(startIds, query, false);
    }

    private <S, E> void profileVertexQuery(List<Long> startIds, Function<Long, Function<GraphTraversalSource, GraphTraversal<S, E>>> query, boolean printMetrics) {
        System.out.println("Profiling query for ids: " + startIds + "\n");
        long totalMs = 0;
        long max = 0;
        long maxId = 0;
        for (long id : startIds) {
            System.out.println("Running query for id: " + id);
            LongLongImmutablePair stat = statsForQuery(query, id, iters, printMetrics);
            long average = stat.leftLong();
            long elements = stat.rightLong();
            double perElement = 1.0 * average / elements;
            totalMs += average;
            if (max < average) {
                max = average;
                maxId = id;
            }
            System.out.printf("Average for id: %d - %dms%n", id, average);
            System.out.printf("Per result: %.2fms%n%n", perElement);
        }
        System.out.printf("Average time: %dms%n", totalMs / startIds.size());
        System.out.printf("Max time: %dms for id %d%n", max, maxId);
    }

    private <S, E> LongLongImmutablePair statsForQuery(Function<Long, Function<GraphTraversalSource, GraphTraversal<S, E>>> query, long id, long iters, boolean printMetrics) {
        long totalMsPerId = 0;
        long elements = 0;
        for (int i = 0; i < iters; i++) {
            System.out.println(i + 1 + "/" + iters);
            TraversalMetrics metrics = profile(query.apply(id));
            if (printMetrics){
                System.out.println(metrics);
            }
            elements = getLastMetric(metrics).getCount("elementCount");
            long durationMs = metrics.getDuration(TimeUnit.MILLISECONDS);
            System.out.println("Finished in: " + durationMs + "ms. Results: " + elements);
            totalMsPerId += durationMs;
        }
        return new LongLongImmutablePair(totalMsPerId / iters, elements);
    }

    private Metrics getLastMetric(TraversalMetrics metrics) {
        List<? extends Metrics> metrics1 = new ArrayList<>(metrics.getMetrics());
        return metrics1.get(metrics1.size() - 1);
    }

    private void runQueryByName(String name, long arg) {
        String label;
        if (name.equals("earliestContainingCommit")) {
            label = "CNT";
        } else if (name.equals("recursiveContentPathsWithPermissions")) {
            label = "REV";
        } else if (name.equals("snapshotRevisionsWithBranches")) {
            label = "SNP";
        } else {
            System.out.println("Unknown query name: " + name);
            return;
        }

        boolean printMetrics = false;
        List<Long> startIds;
        if (arg != -1) {
            System.out.println("Argument provided, running query once for id: " + arg);
            startIds = List.of(arg);
            printMetrics = true;
        } else {
            System.out.println("Generating starting points...");
            startIds = randomVerticesWithLabel(label, samples);
        }
        if (name.equals("earliestContainingCommit")) {
            profileVertexQuery(startIds, Query::earliestContainingCommit, printMetrics);
        } else if (name.equals("recursiveContentPathsWithPermissions")) {
            profileVertexQuery(startIds, Query::recursiveContentPathsWithPermissions, printMetrics);
        } else if (name.equals("snapshotRevisionsWithBranches")) {
            profileVertexQuery(startIds, Query::snapshotRevisionsWithBranches, printMetrics);
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
