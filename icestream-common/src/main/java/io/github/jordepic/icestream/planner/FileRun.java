package io.github.jordepic.icestream.planner;

/**
 * A contiguous run of files within a table to process - all the same FileKind.
 */
public sealed interface FileRun permits DataFileRun, EqualityDeleteFileRun {
    FileKind kind();

    long maxSeq();
}
