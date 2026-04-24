package io.github.jordepic.icestream.indexer;

import java.io.Serializable;

/**
 * JavaBean holding one Cassandra index row. Field names follow JavaBean camelCase; the Cassandra
 * connector's default JavaBeanColumnMapper maps them to the snake_case table columns.
 */
public final class IndexRow implements Serializable {

    private static final long serialVersionUID = 1L;

    private int specId;
    private byte[] partitionKey;
    private int bucket;
    private byte[] pk;
    private String dataFilePath;
    private long pos;

    public IndexRow() {}

    public IndexRow(int specId, byte[] partitionKey, int bucket, byte[] pk, String dataFilePath, long pos) {
        this.specId = specId;
        this.partitionKey = partitionKey;
        this.bucket = bucket;
        this.pk = pk;
        this.dataFilePath = dataFilePath;
        this.pos = pos;
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

    public byte[] getPk() {
        return pk;
    }

    public void setPk(byte[] pk) {
        this.pk = pk;
    }

    public String getDataFilePath() {
        return dataFilePath;
    }

    public void setDataFilePath(String dataFilePath) {
        this.dataFilePath = dataFilePath;
    }

    public long getPos() {
        return pos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }
}
