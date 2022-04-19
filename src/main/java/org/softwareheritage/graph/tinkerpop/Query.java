package org.softwareheritage.graph.tinkerpop;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.function.Function;

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
     * Finds all revisions, which contain the provided dir/content vertex.
     *
     * @param v the id of the dir/content vertex.
     * @return all containing revision vertices.
     */
    public static Function<GraphTraversalSource, GraphTraversal<Vertex, Vertex>> containingRevisions(long v) {
        return g -> g.withSideEffect("a", new HashSet<>())
                     .V(v)
                     .repeat(__.in().dedup().where(P.without("a")).aggregate("a"))
                     .emit(__.hasLabel("REV"))
                     .dedup();
    }

    /**
     * Finds up to {@code limit} earliest revisions, which contain the provided dir/content vertex.
     *
     * @param v     the id of the dir/content vertex.
     * @param limit the number of revisions to find.
     * @return up to {@code limit} earliest revisions, containing the specified dir/content vertex.
     */
    public static Function<GraphTraversalSource, GraphTraversal<Vertex, Vertex>> earliestContainingRevisions(long v, long limit) {
        return containingRevisions(v).andThen(g -> g.order().by("author_timestamp", Order.asc).limit(limit));
    }

    /**
     * Find the earliest containing revision of the provided dir/content vertex.
     *
     * @param v the id of the dir/content vertex.
     * @return earliest containing revision vertex.
     */
    public static Function<GraphTraversalSource, GraphTraversal<Vertex, Vertex>> earliestContainingRevision(long v) {
        return earliestContainingRevisions(v, 1);
    }

    /**
     * Finds an origin of the earliest containing revision of the provided dir/content vertex.
     *
     * @param revision the id of the dir/content vertex.
     * @return an origin vertex.
     */
    public static Function<GraphTraversalSource, GraphTraversal<Vertex, Vertex>> originOfRevision(long revision) {
        return g -> g.withSideEffect("a", new HashSet<>())
                     .V(revision)
                     .repeat(__.in().dedup().where(P.without("a")).aggregate("a"))
                     .until(__.hasLabel("ORI"));
    }

    /**
     * Finds all revisions, which contain the provided dir/content vertex and are older than the given threshold.
     *
     * @param v   the id of the dir/content vertex.
     * @param max limit for revision time.
     * @return all containing revision vertices.
     */
    public static Function<GraphTraversalSource, GraphTraversal<Vertex, Vertex>> revisionsEarlierThan(long v, long max) {
        return g -> g.withSideEffect("a", new HashSet<>())
                     .V(v)
                     .repeat(__.in().dedup().where(P.without("a")).aggregate("a"))
                     .emit(__.hasLabel("REV").has("author_timestamp", P.lt(max)))
                     .dedup();
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
                     .repeat(__.outE()
                               .inV().choose(__.hasLabel("REV"), __.out().hasLabel("DIR")))
                     .emit(__.hasLabel("DIR", "CNT"))
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
                         StringBuilder dir = new StringBuilder();
                         for (int i = 0; i < pathDirs.size() - 1; i++) {
                             dir.append(pathDirs.get(i)[0].filename) // parent path should not have duplicate edges
                                .append("/");
                         }
                         SwhProperties.DirEntryString[] last = pathDirs.get(pathDirs.size() - 1);
                         if (last.length == 1) {
                             var entry = last[0];
                             return List.of(String.format("%s%s [perms: %s]", dir, entry.filename, entry.permission))
                                        .iterator();
                         }
                         List<String> res = new ArrayList<>();
                         for (SwhProperties.DirEntryString entry : last) {
                             res.add(String.format("%s%s [perms: %s]", dir, entry.filename, entry.permission));
                         }
                         return res.iterator();
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
                               .inV().hasLabel("REV", "REL"))
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
                         String outLabel = (String) ((Map<Object, Object>) edgeElementMap.get(Direction.OUT)).get(
                                 T.label);

                         String edgeStr = String.format("(%s -> %s)", outId, inId);
                         if (outLabel.equals("SNP")) {
                             String[] branches = (String[]) edgeElementMap.get("filenames");
                             List<String> res = new ArrayList<>(branches.length);
                             for (String branch : branches) {
                                 res.add(edgeStr + " " + branch);
                             }
                             return res.iterator();
                         }
                         return List.of(edgeStr).iterator();
                     })
        );
    }
}
