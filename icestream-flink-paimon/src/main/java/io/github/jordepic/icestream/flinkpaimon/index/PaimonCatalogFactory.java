package io.github.jordepic.icestream.flinkpaimon.index;

import java.util.Map;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

/**
 * Builds a Paimon {@link Catalog} from a small option map. The master pod creates one of these
 * at startup and shares it across all per-table runs.
 *
 * <p>The default {@code metastore = filesystem} is right for icestream's deployment shape: index
 * tables live in object storage alongside the iceberg warehouse and don't need a Hive Metastore.
 * Tests use {@code filesystem} pointed at a local temp directory; production points the
 * {@code warehouse} at an {@code s3://...} URL with the standard {@code s3.endpoint},
 * {@code s3.access-key}, {@code s3.secret-key} options for MinIO.
 */
public final class PaimonCatalogFactory {

    private PaimonCatalogFactory() {}

    public static Catalog create(String warehousePath, Map<String, String> extraOptions) {
        Options options = new Options();
        options.set(CatalogOptions.WAREHOUSE, warehousePath);
        extraOptions.forEach(options::set);
        return org.apache.paimon.catalog.CatalogFactory.createCatalog(CatalogContext.create(options));
    }
}
