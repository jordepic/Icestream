package io.github.jordepic.icestream.flinkpaimon.index;

import io.github.jordepic.icestream.index.IndexBackend;
import io.github.jordepic.icestream.schema.IcestreamTableConfig;
import java.util.regex.Pattern;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Catalog.DatabaseAlreadyExistException;
import org.apache.paimon.catalog.Catalog.DatabaseNotExistException;
import org.apache.paimon.catalog.Catalog.TableAlreadyExistException;
import org.apache.paimon.catalog.Catalog.TableNotExistException;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;

/**
 * Paimon-backed {@link IndexBackend}: maintains one Paimon primary-key table per iceberg table
 * inside a single Paimon database. Mirrors the surface of the Spark+Cassandra
 * {@code CassandraIndex} so callers (TableProcessor, FlinkDataFileIndexer,
 * FlinkDeleteFileCreator) barely change shape.
 *
 * <p>Iceberg table identifiers can contain dots (separating namespaces from the table name);
 * Paimon identifiers are flat {@code (database, table)} pairs, so dots and other non-ident
 * characters in the iceberg path are flattened to underscores when forming the Paimon table
 * name. The {@link #database()} stays fixed; namespace structure isn't replicated.
 */
public final class PaimonIndex implements IndexBackend {

    private static final Pattern ILLEGAL_IDENT_CHARS = Pattern.compile("[^a-zA-Z0-9_]");

    private final Catalog catalog;
    private final String database;
    private final java.util.Map<String, String> catalogOptionsForFlink;

    public PaimonIndex(Catalog catalog, String database, java.util.Map<String, String> catalogOptionsForFlink) {
        this.catalog = catalog;
        this.database = database;
        this.catalogOptionsForFlink = java.util.Map.copyOf(catalogOptionsForFlink);
    }

    /**
     * Build a {@link PaimonIndex} together with the catalog it wraps; the warehouse path is
     * captured as part of {@link #catalogOptionsForFlink()} so the converter can re-register the
     * catalog with Flink's table environment.
     */
    public static PaimonIndex create(
            String warehousePath, String database, java.util.Map<String, String> extraOptions) {
        Catalog catalog = PaimonCatalogFactory.create(warehousePath, extraOptions);
        java.util.Map<String, String> flinkOptions = new java.util.HashMap<>(extraOptions);
        flinkOptions.put("warehouse", warehousePath);
        return new PaimonIndex(catalog, database, flinkOptions);
    }

    public Catalog catalog() {
        return catalog;
    }

    public String database() {
        return database;
    }

    /**
     * Options to feed into Flink's {@code CREATE CATALOG ... WITH (...)} when registering this
     * Paimon catalog with a {@code TableEnvironment}. Includes the warehouse path and any
     * filesystem-specific options the catalog was constructed with.
     */
    public java.util.Map<String, String> catalogOptionsForFlink() {
        return catalogOptionsForFlink;
    }

    public Identifier identifierFor(TableIdentifier icebergTableId) {
        return Identifier.create(database, tableName(icebergTableId));
    }

    public String tableName(TableIdentifier icebergTableId) {
        return ILLEGAL_IDENT_CHARS.matcher(icebergTableId.toString()).replaceAll("_");
    }

    public String qualifiedTableName(TableIdentifier icebergTableId) {
        return database + "." + tableName(icebergTableId);
    }

    public Table load(TableIdentifier icebergTableId) {
        try {
            return catalog.getTable(identifierFor(icebergTableId));
        } catch (TableNotExistException e) {
            throw new IllegalStateException(
                    "Paimon index table " + qualifiedTableName(icebergTableId) + " does not exist", e);
        }
    }

    @Override
    public void initializeForTable(TableIdentifier icebergTableId, IcestreamTableConfig config) {
        ensureDatabase();
        try {
            catalog.createTable(identifierFor(icebergTableId), IndexTableSchema.build(config), true);
        } catch (TableAlreadyExistException e) {
            throw new IllegalStateException("Race creating " + qualifiedTableName(icebergTableId), e);
        } catch (DatabaseNotExistException e) {
            throw new IllegalStateException("Paimon database " + database + " disappeared after createDatabase", e);
        }
    }

    private void ensureDatabase() {
        try {
            catalog.createDatabase(database, true);
        } catch (DatabaseAlreadyExistException e) {
            throw new IllegalStateException("Race creating Paimon database " + database, e);
        }
    }
}
