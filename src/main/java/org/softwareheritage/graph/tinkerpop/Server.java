package org.softwareheritage.graph.tinkerpop;

import org.softwareheritage.graph.SwhBidirectionalGraph;
import org.webgraph.tinkerpop.WebgraphGremlinQueryExecutor;
import org.webgraph.tinkerpop.structure.WebGraphGraph;
import org.webgraph.tinkerpop.structure.provider.SimpleWebGraphPropertyProvider;

import java.io.IOException;

public class Server {

    public static void main(String[] args) throws IOException {
        if (args == null || args.length < 3 || args[0] == null || args[1] == null) {
            System.out.println(
                    "Usage: org.webgraph.tinkerpop.server.Server <graph_path> <query> [--profile]");
            return;
        }
        String path = args[0];
        String query = args[1];
        boolean profile = args.length == 4 && args[3].equals("--profile");
        SwhBidirectionalGraph graph = SwhBidirectionalGraph.loadLabelled(path);
        SimpleWebGraphPropertyProvider swh = SwhProperties.getProvider(graph);
        try (var gg = WebGraphGraph.open(graph, swh, path)) {
            System.out.println("Opened graph: " + path);
            var executor = new WebgraphGremlinQueryExecutor(gg);
            if (profile) {
                executor.profile(query);
            } else {
                executor.print(query);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
