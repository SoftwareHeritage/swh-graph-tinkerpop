package org.softwareheritage.graph.tinkerpop;

import org.softwareheritage.graph.SwhBidirectionalGraph;
import org.webgraph.tinkerpop.structure.provider.WebGraphPropertyProvider;

public class SwhWebGraphPropertyProvider implements WebGraphPropertyProvider {
    private final SwhBidirectionalGraph graph;

    public SwhWebGraphPropertyProvider(SwhBidirectionalGraph graph) {
        this.graph = graph;
    }

    @Override
    public String vertexLabel(long nodeId) {
        return graph.getNodeType(nodeId).toString();
    }

    @Override
    public String[] vertexProperties(long nodeId) {
        return new String[]{"author_timestamp"};
    }

    @Override
    public Object vertexProperty(String key, long nodeId) {
        if (!"author_timestamp".equals(key)) {
            throw new RuntimeException("Unknown property key: " + key);
        }
        long authorTimestamp = graph.getAuthorTimestamp(nodeId);
        return authorTimestamp == Long.MIN_VALUE ? null : authorTimestamp;
    }

    @Override
    public String[] edgeProperties(long fromId, long toId) {
        return new String[]{"dir_entry"};
    }

    @Override
    public String edgeLabel(long fromId, long toId) {
        return "edge";
    }

    @Override
    public Object edgeProperty(String key, long fromId, long toId) {
        if (!key.equals("dir_entry")) {
            throw new RuntimeException("Unknown property key: " + key);
        }
        var s = graph.labelledSuccessors(fromId);
        long succ;
        while ((succ = s.nextLong()) != -1) {
            if (succ == toId) {
                return s.label().get();
            }
        }
        return null;
    }
}
