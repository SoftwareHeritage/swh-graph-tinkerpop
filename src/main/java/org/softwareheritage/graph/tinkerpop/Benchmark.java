package org.softwareheritage.graph.tinkerpop;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.longs.LongLongImmutablePair;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.softwareheritage.graph.SwhBidirectionalGraph;
import org.webgraph.tinkerpop.WebgraphGremlinQueryExecutor;
import org.webgraph.tinkerpop.structure.WebGraphGraph;
import org.webgraph.tinkerpop.structure.provider.SimpleWebGraphPropertyProvider;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

public class Benchmark {

    private static final String EXAMPLE = "src/main/resources/example/example";
    private static final String PYTHON_3K = "src/main/resources/python3kcompress/graph";

    private final WebGraphGraph graph;
    private final WebgraphGremlinQueryExecutor e;
    private final long samples;
    private final int iters;

    private final Map<String, Supplier<BenchmarkQuery>> queries = Map.of(
            "earliestContainingCommit", EarliestContainingCommit::new,
            "OriginOfEarliestContainingRevision", OriginOfEarliestContainingRevision::new,
            "recursiveContentPathsWithPermissions", RecursiveContentPathsWithPermissions::new,
            "snapshotRevisionsWithBranches", SnapshotRevisionsWithBranches::new);

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

    /*
    2021-snapshot
        311702.txt
     */
    private <S, E> void profileVertexQuery(List<Long> startIds, BenchmarkQuery query, boolean printMetrics) throws IOException {
        System.out.println("Profiling query for ids: " + startIds + "\n");
        Path dir = Path.of("benchmarks")
                       .resolve(Instant.now().truncatedTo(ChronoUnit.SECONDS).toString() + "-" + query.getName());
        Files.createDirectories(dir);
        System.out.println("Results will be saved at: " + dir);
        long totalMs = 0;
        long max = 0;
        long maxId = 0;
        StringBuilder csvLine = new StringBuilder("id,elements");
        for (int i = 0; i < iters; i++) {
            csvLine.append(",").append("run").append(i + 1);
        }
        try (BufferedWriter bw = Files.newBufferedWriter(dir.resolve("table.csv"), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE)) {
            bw.write(csvLine.append("\n").toString());
        }
        for (long id : startIds) {
            System.out.println("Running query for id: " + id);

            LongLongImmutablePair stat = statsForQuery(query.getQuery(), id, iters, printMetrics, dir);
            long average = stat.leftLong();
            long elements = stat.rightLong();
            double perElement = elements !=0 ? 1.0 * average / elements : 0;
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

    private <S, E> LongLongImmutablePair statsForQuery(Function<Long, Function<GraphTraversalSource, GraphTraversal<S, E>>> query, Long id, long iters, boolean printMetrics, Path dir) throws IOException {
        long totalMsPerId = 0;
        long elements = -1;

        StringBuilder csvLine = new StringBuilder(id.toString());
        Path idDir = dir.resolve(Long.toString(id));
        Files.createDirectories(idDir);
        for (int i = 0; i < iters; i++) {
            System.out.println(i + 1 + "/" + iters);
            TraversalMetrics metrics = profile(query.apply(id));
            if (printMetrics) {
                System.out.println(metrics);
            }
            try (BufferedWriter bw = Files.newBufferedWriter(idDir.resolve(i + 1 + ".txt"), StandardCharsets.UTF_8)) {
                bw.write(metrics.toString());
            }
            Long elementCount = getLastMetric(metrics).getCount("elementCount");
            if (elements == -1) {
                elements = elementCount != null ? elementCount : 0;
                csvLine.append(",").append(elements);
            }
            long durationMs = metrics.getDuration(TimeUnit.MILLISECONDS);
            csvLine.append(",").append(durationMs);
            System.out.println("Finished in: " + durationMs + "ms. Results: " + elements);
            totalMsPerId += durationMs;
        }
        try (BufferedWriter bw = Files.newBufferedWriter(dir.resolve("table.csv"), StandardCharsets.UTF_8,
                StandardOpenOption.APPEND)) {
            bw.write(csvLine.append("\n").toString());
        }
        return new LongLongImmutablePair(totalMsPerId / iters, elements);
    }

    private Metrics getLastMetric(TraversalMetrics metrics) {
        List<? extends Metrics> metrics1 = new ArrayList<>(metrics.getMetrics());
        return metrics1.get(metrics1.size() - 1);
    }

    private void runQueryByName(String name, long arg) throws IOException {
        if (!queries.containsKey(name)) {
            System.out.println("Unknown query name: " + name);
            return;
        }
        BenchmarkQuery query = queries.get(name).get();

        boolean printMetrics = false;
        List<Long> startIds;
        if (arg != -1) {
            System.out.println("Argument provided, running query once for id: " + arg);
            startIds = List.of(arg);
            printMetrics = true;
        } else {
            System.out.println("Generating starting points...");
            startIds = query.generateStartingPoints();
        }
        profileVertexQuery(startIds, query, printMetrics);
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


    private class EarliestContainingCommit implements BenchmarkQuery<Long, Vertex, Vertex> {
        @Override
        public String getName() {
            return "earliestContainingCommit";
        }

        @Override
        public Function<Long, Function<GraphTraversalSource, GraphTraversal<Vertex, Vertex>>> getQuery() {
            return Query::earliestContainingCommit;
        }

        @Override
        public List<Long> generateStartingPoints() {
            return randomVerticesWithLabel("CNT", samples);
        }
    }

    private class OriginOfEarliestContainingRevision implements BenchmarkQuery<Long, Vertex, Vertex> {
        @Override
        public String getName() {
            return "originOfEarliestContainingRevision";
        }

        @Override
        public Function<Long, Function<GraphTraversalSource, GraphTraversal<Vertex, Vertex>>> getQuery() {
            return Query::originOfEarliestContainingRevision;
        }

        @Override
        public List<Long> generateStartingPoints() {
            return randomVerticesWithLabel("REV", samples);
        }
    }

    private class RecursiveContentPathsWithPermissions implements BenchmarkQuery<Long, Vertex, String> {
        @Override
        public String getName() {
            return "recursiveContentPathsWithPermissions";
        }

        @Override
        public Function<Long, Function<GraphTraversalSource, GraphTraversal<Vertex, String>>> getQuery() {
            return Query::recursiveContentPathsWithPermissions;
        }

        @Override
        public List<Long> generateStartingPoints() {
            return randomVerticesWithLabel("REV", samples);
        }
    }

    private class SnapshotRevisionsWithBranches implements BenchmarkQuery<Long, Vertex, String> {
        @Override
        public String getName() {
            return "snapshotRevisionsWithBranches";
        }

        @Override
        public Function<Long, Function<GraphTraversalSource, GraphTraversal<Vertex, String>>> getQuery() {
            return Query::snapshotRevisionsWithBranches;
        }

        @Override
        public List<Long> generateStartingPoints() {
            return randomVerticesWithLabel("SNP", samples);
        }
    }

    interface BenchmarkQuery<T, S, E> {
        String getName();

        Function<T, Function<GraphTraversalSource, GraphTraversal<S, E>>> getQuery();

        List<T> generateStartingPoints();
    }

}
