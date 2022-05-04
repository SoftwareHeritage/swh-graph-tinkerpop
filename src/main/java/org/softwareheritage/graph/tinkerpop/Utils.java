package org.softwareheritage.graph.tinkerpop;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;

public class Utils {
    private static final long BYTE_TO_MB_CONVERSION_VALUE = 1024 * 1024;

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

    public static long getHeapMemoryUsage() {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() / BYTE_TO_MB_CONVERSION_VALUE;
    }
}
