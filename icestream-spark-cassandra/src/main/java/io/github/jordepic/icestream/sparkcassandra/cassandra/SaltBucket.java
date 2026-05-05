package io.github.jordepic.icestream.sparkcassandra.cassandra;

import com.google.common.hash.Hashing;

/**
 * Per-row salt bucket used by the Cassandra-backed index to spread one iceberg partition's
 * rows across {@code N = icestream.index-buckets} Cassandra partition keys. Pure helper —
 * the value lives only in Cassandra; Paimon-backed implementations don't need it.
 */
public final class SaltBucket {

    private SaltBucket() {}

    public static int bucket(byte[] pkBytes, int bucketCount) {
        int hash = Hashing.murmur3_32_fixed().hashBytes(pkBytes).asInt();
        return Math.floorMod(hash, bucketCount);
    }
}
