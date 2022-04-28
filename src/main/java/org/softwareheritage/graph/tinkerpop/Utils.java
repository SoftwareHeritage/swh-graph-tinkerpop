package org.softwareheritage.graph.tinkerpop;

import java.time.Duration;
import java.time.Instant;

public class Utils {
    public static long time(Runnable r) {
        return time(r, true);
    }

    public static long time(Runnable r, boolean print) {
        Instant start = Instant.now();
        r.run();
        long millis = Duration.between(start, Instant.now()).toMillis();
        if (print) {
            System.out.printf("Finished in: %.2fs%n", 1.0 * millis / 1000);
        }
        return millis;
    }
}
