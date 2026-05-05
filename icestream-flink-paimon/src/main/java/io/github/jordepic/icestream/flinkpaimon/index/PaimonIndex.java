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

    public PaimonIndex(Catalog catalog, String database) {
        this.catalog = catalog;
        this.database = database;
    }

    public Catalog catalog() {
        return catalog;
    }

    public String database() {
        return database;
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
