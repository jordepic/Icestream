package io.github.jordepic.icestream.planner;

public record State(long sequenceNumber, FileKind fileKind) implements Comparable<State> {

    public static final State INITIAL = new State(0L, FileKind.DATA);

    @Override
    public int compareTo(State other) {
        int bySeq = Long.compare(sequenceNumber, other.sequenceNumber);
        if (bySeq != 0) {
            return bySeq;
        }
        if (fileKind == other.fileKind) {
            return 0;
        }
        return fileKind == FileKind.EQ_DEL ? -1 : 1;
    }
}
