package org.softwareheritage.graph.tinkerpop;

import it.unimi.dsi.big.util.MappedFrontCodedStringBigList;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.softwareheritage.graph.SwhBidirectionalGraph;
import org.softwareheritage.graph.labels.DirEntry;
import org.webgraph.tinkerpop.structure.property.edge.ArcLabelEdgeProperty;
import org.webgraph.tinkerpop.structure.property.edge.ArcLabelEdgeSubProperty;
import org.webgraph.tinkerpop.structure.property.edge.ArcLabelEdgeSubPropertyGetter;
import org.webgraph.tinkerpop.structure.property.vertex.VertexProperty;
import org.webgraph.tinkerpop.structure.property.vertex.file.FileVertexProperty;
import org.webgraph.tinkerpop.structure.provider.SimpleWebGraphPropertyProvider;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class SwhProperties {

    private static MappedFrontCodedStringBigList edgeLabelNames;

    private static void loadEdgeLabelNames(String path) throws IOException {
        try {
            edgeLabelNames = MappedFrontCodedStringBigList.load(path + ".labels.fcl");
        } catch (ConfigurationException e) {
            throw new IOException(e);
        }
    }

    public static SimpleWebGraphPropertyProvider getProvider(SwhBidirectionalGraph graph) throws IOException {
        String path = graph.getPath();
        SimpleWebGraphPropertyProvider provider = new SimpleWebGraphPropertyProvider();
        provider.setVertexLabeller(id -> graph.getNodeType(id).toString());
        provider.addVertexProperty(new FileVertexProperty<>("author_timestamp", Long.class,
                Path.of(path + ".property.author_timestamp.bin")));
        provider.addVertexProperty(new VertexProperty<>("swhid", graph::getSWHID));
        return provider;
    }

    public static SimpleWebGraphPropertyProvider withEdgeLabels(SwhBidirectionalGraph graph) throws IOException {
        String path = graph.getPath();
        loadEdgeLabelNames(path);
        SimpleWebGraphPropertyProvider provider = getProvider(graph);
        ArcLabelEdgeProperty<DirEntry[]> edgeProperty = new ArcLabelEdgeProperty<>(graph.getForwardGraph().getLabelledGraph());
        provider.addEdgeProperty(edgeProperty);
        provider.addEdgeProperty(new ArcLabelEdgeSubProperty<>("dir_entry_str", edgeProperty, dirEntryStr()));
        provider.addEdgeProperty(new ArcLabelEdgeSubProperty<>("filenames", edgeProperty, filenames()));
        return provider;
    }

    private static ArcLabelEdgeSubPropertyGetter<DirEntry[], DirEntryString[]> dirEntryStr() {
        return dirEntries -> {
            if (dirEntries.length == 0) {
                return null;
            }
            DirEntryString[] res = new DirEntryString[dirEntries.length];
            for (int i = 0; i < dirEntries.length; i++) {
                res[i] = new DirEntryString(getFilename(dirEntries[i]), dirEntries[i].permission);
            }
            return res;
        };
    }

    private static ArcLabelEdgeSubPropertyGetter<DirEntry[], String[]> filenames() {
        return dirEntries -> {
            if (dirEntries.length == 0) {
                return null;
            }
            String[] res = new String[dirEntries.length];
            for (int i = 0; i < dirEntries.length; i++) {
                res[i] = getFilename(dirEntries[i]);
            }
            return res;
        };
    }


    private static String getFilename(DirEntry dirEntry) {
        return new String(Base64.getDecoder().decode(edgeLabelNames.getArray(dirEntry.filenameId)));
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
