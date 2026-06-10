package io.github.jordepic.icestream.master;

import io.github.jordepic.icestream.schema.IcestreamProperties;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recursively walks a catalog's namespaces and returns the tables that opted into icestream (have
 * {@link IcestreamProperties#PRIMARY_KEYS} set). Shared by the Spark {@code MasterLoop} (which then
 * processes one run per table) and the Flink launcher (which ensures one autonomous job per table).
 */
public final class IcestreamCatalogScan {

    private static final Logger log = LoggerFactory.getLogger(IcestreamCatalogScan.class);

    private IcestreamCatalogScan() {}

    public static List<TableIdentifier> icestreamTables(Catalog catalog) {
        List<TableIdentifier> out = new ArrayList<>();
        for (Namespace ns : walkNamespaces(catalog)) {
            try {
                for (TableIdentifier id : catalog.listTables(ns)) {
                    if (isIcestreamTable(catalog, id)) {
                        out.add(id);
                    }
                }
            } catch (Exception e) {
                log.warn("Failed to list tables in namespace {}; skipping", ns, e);
            }
        }
        return out;
    }

    private static boolean isIcestreamTable(Catalog catalog, TableIdentifier id) {
        try {
            Table t = catalog.loadTable(id);
            return t.properties().containsKey(IcestreamProperties.PRIMARY_KEYS);
        } catch (NoSuchTableException e) {
            return false;
        } catch (Exception e) {
            log.warn("Failed to load {} for icestream-property check; skipping this sweep", id, e);
            return false;
        }
    }

    private static List<Namespace> walkNamespaces(Catalog catalog) {
        if (!(catalog instanceof SupportsNamespaces sn)) {
            return List.of();
        }
        List<Namespace> out = new ArrayList<>();
        walkNamespaces(sn, Namespace.empty(), out);
        return out;
    }

    private static void walkNamespaces(SupportsNamespaces sn, Namespace parent, List<Namespace> out) {
        List<Namespace> children;
        try {
            children = sn.listNamespaces(parent);
        } catch (Exception e) {
            log.warn("Failed to list children of {}; skipping subtree", parent, e);
            return;
        }
        for (Namespace child : children) {
            out.add(child);
            walkNamespaces(sn, child, out);
        }
    }
}
