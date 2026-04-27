package io.github.jordepic.icestream.converter;

import java.io.Serializable;

/**
 * JavaBean carrying the four Cassandra partition+clustering columns we use to look up an
 * (eq-delete pk) → (data_file_path, pos) entry. The Cassandra connector's default
 * JavaBeanColumnMapper maps camelCase fields to the snake_case table columns.
 */
public final class SerializableEqualityDelete implements Serializable {

    private static final long serialVersionUID = 1L;

    private int specId;
    private byte[] partitionKey;
    private int bucket;
    private byte[] serializedDeleteCondition;

    public SerializableEqualityDelete() {}

    public SerializableEqualityDelete(int specId, byte[] partitionKey, int bucket, byte[] serializedDeleteCondition) {
        this.specId = specId;
        this.partitionKey = partitionKey;
        this.bucket = bucket;
        this.serializedDeleteCondition = serializedDeleteCondition;
    }

    public int getSpecId() {
        return specId;
    }

    public void setSpecId(int specId) {
        this.specId = specId;
    }

    public byte[] getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(byte[] partitionKey) {
        this.partitionKey = partitionKey;
    }

    public int getBucket() {
        return bucket;
    }

    public void setBucket(int bucket) {
        this.bucket = bucket;
    }

    public byte[] getSerializedDeleteCondition() {
        return serializedDeleteCondition;
    }

    public void setSerializedDeleteCondition(byte[] serializedDeleteCondition) {
        this.serializedDeleteCondition = serializedDeleteCondition;
    }
}
