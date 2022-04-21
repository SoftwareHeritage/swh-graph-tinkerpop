package org.softwareheritage.graph.tinkerpop;

import it.unimi.dsi.big.util.MappedFrontCodedStringBigList;
import it.unimi.dsi.fastutil.bytes.ByteBigList;
import it.unimi.dsi.fastutil.bytes.ByteMappedBigList;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.fastutil.longs.LongMappedBigList;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.softwareheritage.graph.SwhBidirectionalGraph;
import org.softwareheritage.graph.labels.DirEntry;
import org.webgraph.tinkerpop.structure.property.edge.ArcLabelEdgeProperty;
import org.webgraph.tinkerpop.structure.property.edge.ArcLabelEdgeSubProperty;
import org.webgraph.tinkerpop.structure.property.edge.ArcLabelEdgeSubPropertyGetter;
import org.webgraph.tinkerpop.structure.property.vertex.VertexProperty;
import org.webgraph.tinkerpop.structure.property.vertex.VertexPropertyGetter;
import org.webgraph.tinkerpop.structure.property.vertex.file.FileVertexProperty;
import org.webgraph.tinkerpop.structure.provider.SimpleWebGraphPropertyProvider;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Base64;

public class SwhProperties {

    private static MappedFrontCodedStringBigList edgeLabelNames;
    private static ByteBigList messageBuffer;
    private static LongBigList messageOffsets;

    private static void loadEdgeLabelNames(String path) throws IOException {
        try {
            edgeLabelNames = MappedFrontCodedStringBigList.load(path + ".labels.fcl");
        } catch (ConfigurationException e) {
            throw new IOException(e);
        }
    }

    public static void loadMessages(Path bufferPath, Path offsetPath) throws IOException {
        try (RandomAccessFile bufferFile = new RandomAccessFile(bufferPath.toFile(), "r");
             RandomAccessFile offsetFile = new RandomAccessFile(offsetPath.toFile(), "r")) {
            messageBuffer = ByteMappedBigList.map(bufferFile.getChannel());
            messageOffsets = LongMappedBigList.map(offsetFile.getChannel());
        }
    }

    public static SimpleWebGraphPropertyProvider getProvider(SwhBidirectionalGraph graph) throws IOException {
        String path = graph.getPath();
        loadMessages(Path.of(path + ".property.message.bin"), Path.of(path + ".property.message.offset.bin"));
        SimpleWebGraphPropertyProvider provider = new SimpleWebGraphPropertyProvider();
        provider.setVertexLabeller(id -> graph.getNodeType(id).toString());
        provider.addVertexProperty(new FileVertexProperty<>("author_timestamp", Long.class,
                Path.of(path + ".property.author_timestamp.bin")));
        provider.addVertexProperty(new VertexProperty<>("swhid", graph::getSWHID));
        provider.addVertexProperty(new VertexProperty<>("message", message()));
        return provider;
    }

    public static SimpleWebGraphPropertyProvider withEdgeLabels(SwhBidirectionalGraph graph) throws IOException {
        String path = graph.getPath();
        loadEdgeLabelNames(path);
        SimpleWebGraphPropertyProvider provider = getProvider(graph);
        ArcLabelEdgeProperty<DirEntry[]> edgeProperty = new ArcLabelEdgeProperty<>(
                graph.getForwardGraph().underlyingLabelledGraph());
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

    public static VertexPropertyGetter<String> message() {
        return nodeId -> {
            if (messageBuffer == null || messageOffsets == null) {
                throw new IllegalStateException("Messages not loaded");
            }
            long startOffset = messageOffsets.getLong(nodeId);
            if (startOffset == -1) {
                return null;
            }
            return new String(Base64.getDecoder().decode(getLine(messageBuffer, startOffset)));
        };
    }

    private static byte[] getLine(ByteBigList byteArray, long start) {
        long end = start;
        while (end < byteArray.size64() && byteArray.getByte(end) != '\n') {
            end++;
        }
        int length = (int) (end - start);
        byte[] buffer = new byte[length];
        byteArray.getElements(start, buffer, 0, length);
        return buffer;
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
