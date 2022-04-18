package org.softwareheritage.graph.tinkerpop;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Query {
    /**
     * Finds all leaves (vertices with no outgoing edges) in a subtree rooted at the provided vertex.
     *
     * @implNote uses DFS to traverse the graph, keeps visited vertices in a {@code HashSet}.
     */
    public static Function<GraphTraversalSource, GraphTraversal<Vertex, Vertex>> leaves(long root) {
        return g -> g.withSideEffect("a", new HashSet<>())
                     .V(root)
                     .repeat(__.out().dedup().where(P.without("a")).aggregate("a"))
                     .until(__.not(__.out()));
    }

    /**
     * Finds all commits, which contain the provided dir/file.
     *
     * @param v the id of the dir/file vertex.
     * @return all containing commit vertices.
     */
    public static Function<GraphTraversalSource, GraphTraversal<Vertex, Vertex>> containingCommits(long v) {
        return g -> g.withSideEffect("a", new HashSet<>())
                     .V(v)
                     .repeat(__.in().dedup().where(P.without("a")).aggregate("a"))
                     .emit(__.hasLabel("REV"))
                     .dedup();
    }

    /**
     * Finds up to {@code limit} earliest commits, which contain the provided dir/file.
     *
     * @param v     the id of the dir/file vertex.
     * @param limit the number of commits to find.
     * @return up to {@code limit} earliest commits, containing the specified dir/file.
     */
    public static Function<GraphTraversalSource, GraphTraversal<Vertex, Vertex>> earliestContainingCommits(long v, long limit) {
        return containingCommits(v).andThen(g -> g.order().by("author_timestamp", Order.asc).limit(limit));
    }

    /**
     * Find the earliest containing commit of the provided dir/file.
     *
     * @param v the id of the dir/file vertex.
     * @return earliest containing commit vertex.
     */
    public static Function<GraphTraversalSource, GraphTraversal<Vertex, Vertex>> earliestContainingCommit(long v) {
        return earliestContainingCommits(v, 1);
    }

    /**
     * Finds an origin of the earliest containing revision of the provided dir/file.
     *
     * @param revision the id of the dir/file vertex.
     * @return an origin vertex.
     */
    public static Function<GraphTraversalSource, GraphTraversal<Vertex, Vertex>> originOfEarliestContainingRevision(long revision) {
        return g -> g.withSideEffect("a", new HashSet<>())
                     .withSideEffect("b", new HashSet<>())
                     .V(revision)
                     .repeat(__.in().dedup().where(P.without("a")).aggregate("a"))
                     .emit(__.hasLabel("REV"))
                     .dedup()
                     .repeat(__.in().dedup().where(P.without("b")).aggregate("b"))
                     .until(__.hasLabel("ORI"));
    }

    /**
     * Returns all paths in a revision/directory subtree.
     * <p>
     * If the passed vertex is a revision, makes one step to the associated directory vertex.
     *
     * @param root the revision/directory vertex id
     * @return paths from revision/directory to leaves.
     */
    public static Function<GraphTraversalSource, GraphTraversal<Vertex, Path>> revisionContentPaths(long root) {
        return g -> g.V(root).choose(__.hasLabel("REV"), __.out().hasLabel("DIR"))
                     .repeat(__.outE().inV())
                     .emit()
                     .path();
    }

    /**
     * Lists all file paths with permissions in a subtree for a given revision.
     * Similar to {@code ls -lR}
     *
     * @param revision the revision vertex id
     * @return file paths in a revision subtree
     */
    public static Function<GraphTraversalSource, GraphTraversal<Vertex, String>> recursiveContentPathsWithPermissions(long revision) {
        return revisionContentPaths(revision).andThen(paths ->
                paths.map(__.unfold()
                            .<SwhProperties.DirEntryString[]>values("dir_entry_str")
                            .fold())
                     .flatMap(path -> {
                         List<SwhProperties.DirEntryString[]> pathDirs = path.get();
                         String dir = pathDirs
                                 .stream()
                                 .limit(pathDirs.size() - 1)
                                 .map(dirs -> dirs[0].filename) // parent path should not have duplicate edges
                                 .collect(Collectors.joining("/"));
                         if (!dir.isEmpty()) {
                             dir += "/";
                         }
                         String finalDir = dir;
                         return Arrays.stream(pathDirs.get(pathDirs.size() - 1))
                                      .map(entry ->
                                              String.format("%s%s [perms: %s]", finalDir, entry.filename,
                                                      entry.permission))
                                      .iterator();
                     }));
    }

    /**
     * Returns all edges under a snapshot, pointing to revisions and releases.
     *
     * @param snapshot the root snapshot
     * @return revisions relationships in snapshot subtree
     */
    public static Function<GraphTraversalSource, GraphTraversal<Vertex, Edge>> snapshotRevisions(long snapshot) {
        return g -> g.withSideEffect("e", new HashSet<>())
                     .V(snapshot)
                     .repeat(__.outE()
                               .where(P.without("e"))
                               .aggregate("e")
                               .inV().hasLabel("REV",  "REL"))
                     .until(__.not(__.out().hasLabel("REV", "REL")))
                     .<Edge>cap("e")
                     .unfold();
    }

    /**
     * Lists all snapshot, revision, and release relationships.
     * If the relationship is snp -> *, outputs the branch name.
     *
     * @param snapshot the snapshot to list
     * @return revisions in snapshot subtree
     */
    public static Function<GraphTraversalSource, GraphTraversal<Vertex, String>> snapshotRevisionsWithBranches(long snapshot) {
        return snapshotRevisions(snapshot).andThen(edges ->
                edges.elementMap("filenames")
                     .flatMap(edgeElementMapTraverser -> {
                         Map<Object, Object> edgeElementMap = edgeElementMapTraverser.get();
                         long outId = (long) ((Map<Object, Object>) edgeElementMap.get(Direction.OUT)).get(T.id);
                         long inId = (long) ((Map<Object, Object>) edgeElementMap.get(Direction.IN)).get(T.id);
                         String outLabel = (String) ((Map<Object, Object>) edgeElementMap.get(Direction.OUT)).get(T.label);

                         String edgeStr = String.format("(%s -> %s)", outId, inId);
                         if (outLabel.equals("SNP")) {
                             List<String> branches = (List<String>) edgeElementMap.get("filenames");
                             return branches.stream()
                                            .map(branch -> edgeStr + " " + branch)
                                            .iterator();
                         }
                         return Stream.of(edgeStr).iterator();
                     })
        );
    }
}
