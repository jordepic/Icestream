package io.github.jordepic.icestream.planner;

import java.io.Serializable;

/** A contiguous run of files within a table to process - all the same FileKind. */
public sealed interface FileRun extends Serializable permits DataFileRun, EqualityDeleteFileRun {
    FileKind kind();

    long maxSeq();
}
