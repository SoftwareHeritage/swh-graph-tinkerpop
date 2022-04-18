package org.softwareheritage.graph.tinkerpop;

import java.time.Duration;
import java.time.Instant;

public class Utils {
    public static void time(Runnable r) {
        Instant start = Instant.now();
        r.run();
        System.out.println("Finished in: " + Duration.between(start, Instant.now()).toSeconds() + "s");
    }
}
