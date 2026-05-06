package io.github.jordepic.icestream.master;

import java.util.HashMap;
import java.util.Map;

/**
 * Env-var helpers shared by every backend's {@code Main}. Both {@code SparkMain} and
 * {@code FlinkMain} use these to load their {@code Config} records from the process environment.
 */
public final class EnvConfig {

    private EnvConfig() {}

    /** Read a required env var, throwing {@link IllegalStateException} if missing or blank. */
    public static String requireEnv(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required env var " + key);
        }
        return value;
    }

    /** Read an optional env var, returning the supplied fallback if missing or blank. */
    public static String envOrDefault(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }

    /**
     * Collect every env var beginning with {@code prefix} into a map. The map key is the
     * stripped, lowercase, dot-separated form of the env-var name — e.g.
     * {@code ICESTREAM_PAIMON_OPT_S3_ENDPOINT} → {@code s3.endpoint}. Used by backends to
     * pass free-form catalog options through the helm chart's env-var template.
     */
    public static Map<String, String> optionsFromEnv(String prefix) {
        Map<String, String> out = new HashMap<>();
        System.getenv().forEach((k, v) -> {
            if (k.startsWith(prefix)) {
                String key = k.substring(prefix.length()).toLowerCase().replace('_', '.');
                out.put(key, v);
            }
        });
        return out;
    }
}
