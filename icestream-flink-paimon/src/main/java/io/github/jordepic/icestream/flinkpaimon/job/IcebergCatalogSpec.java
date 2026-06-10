package io.github.jordepic.icestream.flinkpaimon.job;

import java.io.Serializable;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;

/**
 * Serializable recipe for rebuilding the iceberg {@link Catalog} on a TaskManager.
 *
 * <p>Unlike the channel design — where every iceberg interaction stayed on the driver — the
 * autonomous job's source and committer talk to iceberg directly (plan the walk, commit the
 * {@code RowDelta}, advance the watermark) from inside operators. A live {@code Catalog} isn't
 * serializable, so the job ships this spec and each operator rebuilds the catalog in {@code open()}.
 */
public record IcebergCatalogSpec(String impl, String name, Map<String, String> properties)
        implements Serializable {

    private static final long serialVersionUID = 1L;

    public IcebergCatalogSpec {
        properties = Map.copyOf(properties);
    }

    /**
     * Build a fresh live catalog (callers own closing it if it is {@link java.io.Closeable}).
     *
     * <p>{@code load()} runs <em>on the TaskManager</em> (from the source/committer {@code open()}),
     * so the {@link Configuration} is built there and picks up that node's classpath Hadoop config
     * (e.g. {@code core-site.xml} via {@code HADOOP_CONF_DIR}) — which is what an HDFS-backed warehouse
     * needs. For the REST + object-store deployment this targets, Hadoop config is unused: S3FileIO
     * reads its {@code s3.*} settings from {@code properties}, which are shipped with this spec. (To
     * instead pin the driver's Hadoop config, this is the spot to swap in
     * {@code org.apache.iceberg.hadoop.SerializableConfiguration}, the same mechanism iceberg-flink's
     * {@code CatalogLoader} uses.)
     */
    public Catalog load() {
        return CatalogUtil.loadCatalog(impl, name, properties, new Configuration());
    }
}
