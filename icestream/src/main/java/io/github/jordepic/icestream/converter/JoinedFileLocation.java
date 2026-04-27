package io.github.jordepic.icestream.converter;

import java.io.Serializable;

/**
 * JavaBean target for {@code joinWithCassandraTable(...)}'s row reader: the projected
 * {@code (data_file_path, pos)} columns returned for each matched pk lookup.
 */
public final class JoinedFileLocation implements Serializable {

    private static final long serialVersionUID = 1L;

    private String dataFilePath;
    private long pos;

    public JoinedFileLocation() {}

    public JoinedFileLocation(String dataFilePath, long pos) {
        this.dataFilePath = dataFilePath;
        this.pos = pos;
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
