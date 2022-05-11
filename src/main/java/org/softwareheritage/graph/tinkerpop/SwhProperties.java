package org.softwareheritage.graph.tinkerpop;

import it.unimi.dsi.big.util.MappedFrontCodedStringBigList;
import org.softwareheritage.graph.SwhBidirectionalGraph;
import org.softwareheritage.graph.labels.DirEntry;
import org.webgraph.tinkerpop.structure.property.edge.ArcLabelEdgeProperty;
import org.webgraph.tinkerpop.structure.property.edge.ArcLabelEdgeSubProperty;
import org.webgraph.tinkerpop.structure.property.vertex.VertexProperty;
import org.webgraph.tinkerpop.structure.property.vertex.file.FileVertexProperty;
import org.webgraph.tinkerpop.structure.provider.StandardWebGraphPropertyProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

public class SwhProperties {

    public static StandardWebGraphPropertyProvider getProvider(SwhBidirectionalGraph graph) throws IOException {
        String path = graph.getPath();
        graph.loadMessages();
        StandardWebGraphPropertyProvider provider = new StandardWebGraphPropertyProvider();
        provider.setVertexLabeller(id -> graph.getNodeType(id).toString());
        provider.addVertexProperty(new FileVertexProperty<>("author_timestamp", Long.class,
                Path.of(path + ".property.author_timestamp.bin")));
        provider.addVertexProperty(new VertexProperty<>("swhid", graph::getSWHID));
        provider.addVertexProperty(new VertexProperty<>("message", nodeId -> {
            try {
                return graph.getProperties().getMessage(nodeId);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }));
        return provider;
    }

    public static StandardWebGraphPropertyProvider withEdgeLabels(SwhBidirectionalGraph graph) throws IOException {
        graph.loadLabelNames();
        StandardWebGraphPropertyProvider provider = getProvider(graph);
        ArcLabelEdgeProperty<DirEntry[]> edgeProperty = new ArcLabelEdgeProperty<>(
                graph.getForwardGraph().underlyingLabelledGraph());
        provider.addEdgeProperty(edgeProperty);
        provider.addEdgeProperty(
                new ArcLabelEdgeSubProperty<>("dir_entry_str", edgeProperty,
                        dirEntries -> dirEntryStr(dirEntries, graph)));
        provider.addEdgeProperty(
                new ArcLabelEdgeSubProperty<>("filenames", edgeProperty, dirEntries -> filenames(dirEntries, graph)));
        return provider;
    }

    private static DirEntryString[] dirEntryStr(DirEntry[] dirEntries, SwhBidirectionalGraph graph) {
        if (dirEntries.length == 0) {
            return null;
        }
        DirEntryString[] res = new DirEntryString[dirEntries.length];
        for (int i = 0; i < dirEntries.length; i++) {
            res[i] = new DirEntryString(getFilename(dirEntries[i], graph), dirEntries[i].permission);
        }
        return res;
    }

    private static String[] filenames(DirEntry[] dirEntries, SwhBidirectionalGraph graph) {
        if (dirEntries.length == 0) {
            return null;
        }
        String[] res = new String[dirEntries.length];
        for (int i = 0; i < dirEntries.length; i++) {
            res[i] = getFilename(dirEntries[i], graph);
        }
        return res;
    }

    private static String getFilename(DirEntry dirEntry, SwhBidirectionalGraph graph) {
        return new String(graph.getLabelName(dirEntry.filenameId));
    }

    public static class DirEntryString {

        public String filename;
        public int permission;

        public DirEntryString(String filename, int permission) {
            this.filename = filename;
            this.permission = permission;
        }
    }
}
