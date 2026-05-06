package io.github.jordepic.icestream.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shutdown-hook helpers shared by every backend's {@code Main}. Both backends register a JVM
 * shutdown hook that closes its catalog client(s), compute engine, and other long-lived handles
 * with the same "log on failure, never throw" contract.
 */
public final class LifecycleHooks {

    private static final Logger log = LoggerFactory.getLogger(LifecycleHooks.class);

    private LifecycleHooks() {}

    /** Run the closer; log a warn-level message on failure and swallow the exception. */
    public static void closeQuietly(ThrowingRunnable closer, String label) {
        try {
            closer.run();
        } catch (Exception e) {
            log.warn("Error closing {}", label, e);
        }
    }

    /** {@link Runnable} variant that may throw any checked exception. */
    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Exception;
    }
}
