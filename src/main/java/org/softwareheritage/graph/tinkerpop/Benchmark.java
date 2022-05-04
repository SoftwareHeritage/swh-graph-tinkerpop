package org.softwareheritage.graph.tinkerpop;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.fastutil.longs.LongLongImmutablePair;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.SwhBidirectionalGraph;
import org.softwareheritage.graph.labels.DirEntry;
import org.webgraph.tinkerpop.GremlinQueryExecutor;
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
    private final SwhBidirectionalGraph swhGraph;
    private final GremlinQueryExecutor e;
    private final long samples;
    private final int iters;

    private final Map<String, Supplier<BenchmarkQuery>> queries = Map.of(
            "earliestContainingRevision", EarliestContainingRevision::new,
            "originOfRevision", OriginOfRevision::new,
            "recursiveContentPathsWithPermissions", RecursiveContentPathsWithPermissions::new,
            "snapshotRevisionsWithBranches", SnapshotRevisionsWithBranches::new
//            "uniqueOriginVertices", UniqueOriginVertices::new
    );

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
                        new FlaggedOption("cache", JSAP.INTEGER_PARSER, "100000", JSAP.NOT_REQUIRED, 'c', "cache",
                                "The size of edge cache."),
                        new FlaggedOption("argument", JSAP.LONG_PARSER, "-1", JSAP.NOT_REQUIRED, 'a', "argument",
                                "If present, profiles the query with the argument, instead of doing iterations."),
                        new Switch("print", 'p', "print")});

        JSAPResult config = jsap.parse(args);
        if (jsap.messagePrinted()) {
            System.exit(1);
        }

        String path = config.getString("graphPath");
        String query = config.getString("query");
        int iters = config.getInt("iters");
        int samples = config.getInt("samples");
        int cache = config.getInt("cache");
        long argument = config.getLong("argument");
        boolean print = config.getBoolean("print");

        System.out.println("Loading graph...");
        SwhBidirectionalGraph swhGraph = SwhBidirectionalGraph.loadLabelled(path);
        swhGraph.loadLabelNames();
        swhGraph.loadAuthorTimestamps();

        SimpleWebGraphPropertyProvider swh = SwhProperties.withEdgeLabels(swhGraph);
        WebGraphGraph graph = WebGraphGraph.open(swhGraph, swh, path);
        Benchmark benchmark = new Benchmark(graph, swhGraph, samples, iters);
        System.out.println("Done");

        benchmark.runQueryByName(query, argument, print);
    }

    public Benchmark(WebGraphGraph graph, SwhBidirectionalGraph swhGraph, long samples, int iters) {
        this.graph = graph;
        this.swhGraph = swhGraph;
        this.samples = samples;
        this.iters = iters;
        this.e = new GremlinQueryExecutor(graph);
    }

    private <S, E> void profileVertexQuery(List<Long> startIds, BenchmarkQuery query, boolean printMetrics) throws IOException {
        System.out.println("Profiling query for ids: " + startIds + "\n");
        Path dir = Path.of("benchmarks")
                       .resolve(Instant.now().truncatedTo(ChronoUnit.SECONDS).toString() + "-" + query.getName());
        Files.createDirectories(dir);
        System.out.println("Results will be saved at: " + dir);
        long totalMs = 0;
        long totalNative = 0;
        long totalElements = 0;
        long maxMs = 0;
        long maxId = 0;
        long maxElements = 0;
        StringBuilder csvLine = new StringBuilder("id,elements");
        for (int i = 0; i < iters; i++) {
            csvLine.append(",").append("run").append(i + 1);
        }
        csvLine.append(",").append("native");
        try (BufferedWriter bw = Files.newBufferedWriter(dir.resolve("table.csv"), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE)) {
            bw.write(csvLine.append("\n").toString());
        }
        for (int i = 0; i < startIds.size(); i++) {
            long id = startIds.get(i);
            System.out.printf("Running query for id: %d (%d/%d)%n", id, i + 1, startIds.size());

            Stats stat = statsForQuery(query, id, iters, printMetrics, dir);
            long average = stat.average;
            long elements = stat.elements;
            long nativeTime = stat.nativeTime;

            double perElement = elements != 0 ? 1.0 * average / elements : 0;
            totalMs += average;
            totalNative += nativeTime;
            totalElements += elements;
            if (maxMs < average) {
                maxMs = average;
                maxId = id;
                maxElements = elements;
            }
            System.out.printf("Average for id: %d - %dms. Per element: %.2fms (%d elements)%n%n", id, average,
                    perElement, elements);
        }
        System.out.printf("Average time: %dms. Per element: %.2fms%n", totalMs / startIds.size(),
                1.0 * totalMs / totalElements);
        System.out.printf("Max time: %dms for id %d. Per element: %.2fms (%d elements)%n", maxMs, maxId,
                1.0 * maxMs / maxElements, maxElements);
        System.out.printf("Native time: %dms. Per element: %.2fms%n", totalNative / startIds.size(),
                1.0 * totalNative / totalElements);
        System.out.printf("Slower by: %.2f times%n", 1.0 * totalMs / totalNative);
        System.out.println("Results saved at: " + dir);
    }

    private <T, S, E> Stats statsForQuery(BenchmarkQuery<T, S, E> query, T id, long iters, boolean printMetrics, Path dir) throws IOException {
        long totalMsPerId = 0;
        long elements = -1;

        StringBuilder csvLine = new StringBuilder(id.toString());
        Path idDir = dir.resolve(id.toString());
        Files.createDirectories(idDir);
        for (int i = 0; i < iters; i++) {
            System.out.println(i + 1 + "/" + iters);
            TraversalMetrics metrics = profile(query.getQuery().apply(id));
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
        long nativeTime = Utils.time(() -> {
            long output = query.nativeImpl((Long) id);
            System.out.println("Native output: " + output);
        }, false);
        System.out.println("Native time: " + nativeTime + "ms");
        csvLine.append(",").append(nativeTime);
        try (BufferedWriter bw = Files.newBufferedWriter(dir.resolve("table.csv"), StandardCharsets.UTF_8,
                StandardOpenOption.APPEND)) {
            bw.write(csvLine.append("\n").toString());
        }
        return new Stats(totalMsPerId / iters, elements, nativeTime);
    }

    static class Stats {
        long average;
        long elements;
        long nativeTime;

        public Stats(long average, long elements, long nativeTime) {
            this.average = average;
            this.elements = elements;
            this.nativeTime = nativeTime;
        }
    }

    private Metrics getLastMetric(TraversalMetrics metrics) {
        List<? extends Metrics> metrics1 = new ArrayList<>(metrics.getMetrics());
        return metrics1.get(metrics1.size() - 1);
    }

    private void runQueryByName(String name, long arg, boolean print) throws IOException {
        if (!queries.containsKey(name)) {
            System.out.println("Unknown query name: " + name);
            return;
        }
        BenchmarkQuery query = queries.get(name).get();
        boolean printMetrics = false;
        List<Long> startIds;
        if (arg != -1) {
            System.out.println("Argument provided, running query once for id: " + arg);
            if (print) {
                System.out.println("Printing results:\n");
                Function apply = (Function) query.getQuery().apply(arg);
                e.print(apply);
                return;
            }
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


    private class EarliestContainingRevision implements BenchmarkQuery<Long, Vertex, Vertex> {
        @Override
        public String getName() {
            return "earliestContainingRevision";
        }

        @Override
        public Function<Long, Function<GraphTraversalSource, GraphTraversal<Vertex, Vertex>>> getQuery() {
            return Query::earliestContainingRevision;
        }

        @Override
        public List<Long> generateStartingPoints() {
            return randomVerticesWithLabel("CNT", samples);
        }

        @Override
        public long nativeImpl(long id) {
            return dfsVertices(id, new boolean[50_000_000]);
        }

        private long dfsVertices(long child, boolean[] used) {
            used[(int) child] = true;
            var predecessors = swhGraph.predecessors(child);
            long parent;
            long min = Long.MAX_VALUE;
            while ((parent = predecessors.nextLong()) != -1) {
                if (!used[(int) parent]) {
                    if (swhGraph.getNodeType(parent) == Node.Type.REV) {
                        min = Math.min(min, swhGraph.getAuthorTimestamp(parent));
                    }
                    min = Math.min(min, dfsVertices(parent, used));
                }
            }
            return min;
        }
    }

    private class OriginOfRevision implements BenchmarkQuery<Long, Vertex, Vertex> {
        @Override
        public String getName() {
            return "originOfRevision";
        }

        @Override
        public Function<Long, Function<GraphTraversalSource, GraphTraversal<Vertex, Vertex>>> getQuery() {
            return Query::originOfRevision;
        }

        @Override
        public List<Long> generateStartingPoints() {
            return randomVerticesWithLabel("REV", samples);
        }

        @Override
        public long nativeImpl(long id) {
            return dfs(id, new boolean[50_000_000]);
        }

        private long dfs(long child, boolean[] used) {
            used[(int) child] = true;
            if (swhGraph.getNodeType(child) == Node.Type.ORI) {
                return 1;
            }
            var predecessors = swhGraph.predecessors(child);
            long parent;
            long res = 0;
            while ((parent = predecessors.nextLong()) != -1) {
                if (!used[(int) parent]) {
                    res++;
                    res += dfs(parent, used);
                }
            }
            return res;
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

        @Override
        public long nativeImpl(long id) {
            return dfsVertices(id, new boolean[50_000_000], "");
        }

        private long dfsVertices(long parent, boolean[] used, String path) {
            used[(int) parent] = true;
            if (swhGraph.getNodeType(parent) == Node.Type.REV) {
                LazyLongIterator successors = swhGraph.successors(parent);
                long child;
                while ((child = successors.nextLong()) != -1) {
                    if (swhGraph.getNodeType(child) == Node.Type.DIR) {
                        return dfsVertices(child, used, path);
                    }
                }
            }
            var successors = swhGraph.labelledSuccessors(parent);
            long child;
            long res = 0;
            while ((child = successors.nextLong()) != -1) {
                DirEntry[] label = (DirEntry[]) successors.label().get();
                if (!used[(int) child]) {
                    if (swhGraph.getNodeType(child) == Node.Type.DIR || swhGraph.getNodeType(child) == Node.Type.CNT) {
                        for (DirEntry dirEntry : label) {
                            res++;
                            String path1 = path + "/" + new String(swhGraph.getLabelName(dirEntry.filenameId));
//                        System.out.println(path1);
                            res += dfsVertices(child, used, path1);
                        }
                    } else {
                        res += dfsVertices(child, used, path);
                    }
                }
            }
            return res;
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

        @Override
        public long nativeImpl(long id) {
            return dfsEdges(id, new boolean[50_000_000]);
        }

        private long dfsEdges(long parent, boolean[] used) {
            used[(int) parent] = true;
            var successors = swhGraph.labelledSuccessors(parent);
            long child;
            long res = 0;
            while ((child = successors.nextLong()) != -1) {
                if (swhGraph.getNodeType(child) == Node.Type.REL || swhGraph.getNodeType(child) == Node.Type.REV) {
                    res += 1;
                    mapEdge(parent, child, (DirEntry[]) successors.label().get());
                    if (!used[(int) child]) {
                        res += dfsEdges(child, used);
                    }
                }
            }
            return res;
        }

        private void mapEdge(long outId, long inId, DirEntry[] dirEntries) {
            String edgeStr = String.format("(%s -> %s)", outId, inId);
            if (swhGraph.getNodeType(outId) == Node.Type.SNP) {
                for (DirEntry branch : dirEntries) {
                    String s = edgeStr + " " + new String(swhGraph.getLabelName(branch.filenameId));
//                    System.out.println(s);
                }
                return;
            }
//            System.out.println(edgeStr);
        }

    }
//
//    private class UniqueOriginVertices implements BenchmarkQuery<Long, Vertex, Vertex> {
//        @Override
//        public String getName() {
//            return "uniqueOriginVertices";
//        }
//
//        @Override
//        public Function<Long, Function<GraphTraversalSource, GraphTraversal<Vertex, Vertex>>> getQuery() {
//            return Query::uniqueOriginVertices;
//        }
//
//        @Override
//        public List<Long> generateStartingPoints() {
//            return randomVerticesWithLabel("ORI", samples);
//        }
//    }

    interface BenchmarkQuery<T, S, E> {
        String getName();

        Function<T, Function<GraphTraversalSource, GraphTraversal<S, E>>> getQuery();

        List<T> generateStartingPoints();

        long nativeImpl(long id);
    }

}
