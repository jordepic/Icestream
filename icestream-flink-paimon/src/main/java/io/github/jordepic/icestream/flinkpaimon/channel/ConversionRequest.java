package io.github.jordepic.icestream.flinkpaimon.channel;

import io.github.jordepic.icestream.converter.EqDeleteWorkItem;
import java.io.Serializable;
import java.util.List;

/**
 * One conversion submitted to the standing streaming job via {@link InProcessConversionChannel}:
 * the eq-delete files to read, join against the index, and convert to positional deletes.
 * {@code conversionId} correlates the result back to the waiting caller.
 */
public record ConversionRequest(long conversionId, List<EqDeleteWorkItem> workItems) implements Serializable {

    private static final long serialVersionUID = 1L;
}
