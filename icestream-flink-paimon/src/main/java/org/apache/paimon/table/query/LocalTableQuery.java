/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// ICESTREAM MONKEY PATCH (same-FQN copy of Paimon 1.4.1's LocalTableQuery, shadowing the bundled
// class via classpath order). Wires RemoteLookupFileManager on the read path so a paged-out lookup
// SST is DOWNLOADED from the warehouse rather than rebuilt from the data file.
//
// This is now UPSTREAM in Paimon master (1.5-SNAPSHOT's LocalTableQuery wires the same downloader).
// TODO: delete this file + its cluster-jar shade-exclude and bump paimon.version once 1.5.0 is
// released. We can't pin the snapshot in the meantime: Apache deploys flink connectors only up to
// paimon-flink-1.20, so paimon-flink-2.0:1.5-SNAPSHOT isn't published — a snapshot pin wouldn't
// resolve for CI/teammates without a local `mvn install` of the Paimon master checkout.
package org.apache.paimon.table.query;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.LookupFile;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.mergetree.lookup.LookupSerializerFactory;
import org.apache.paimon.mergetree.lookup.PersistValueProcessor;
import org.apache.paimon.mergetree.lookup.RemoteLookupFileManager;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.paimon.lookup.LookupStoreFactory.bfGenerator;
import static org.apache.paimon.mergetree.LookupFile.localFilePrefix;

/**
 * Implementation for {@link TableQuery} for caching data and file in local.
 *
 * <p><b>icestream monkey patch</b> (same-FQN shadow of Paimon 1.4.1's class). Verbatim copy of the
 * upstream source with ONE behavioral addition: when {@code lookup.remote-file.enabled} is set, wire a
 * {@link RemoteLookupFileManager} into each bucket's {@link LookupLevels}. Stock Paimon never sets a
 * {@code RemoteFileDownloader} on the read path, so {@code LookupLevels.tryToDownloadRemoteSst} is a
 * no-op and a paged-out lookup file is always <em>rebuilt</em> from the data file (read from object
 * store). With the downloader wired, the read path instead <em>downloads</em> the remote sst — which
 * is the whole point of {@code lookup.remote-file} for huge tables. The {@code RemoteLookupFileManager}
 * constructor self-registers via {@code lookupLevels.setRemoteFileDownloader(this)}.
 *
 * <p>This works only because the index table is DV-off (so the write-built ssts use the "value"
 * processor that matches this read path's {@link PersistValueProcessor}); otherwise the processor-id
 * check in {@code LookupLevels.remoteSst} rejects the remote sst and falls back to rebuild.
 *
 * <p>Keep in lock-step with the pinned Paimon version. The cluster-jar shade excludes Paimon's copy
 * of this class so ours wins on the TaskManager classpath; locally, project classes precede the jar.
 */
public class LocalTableQuery implements TableQuery {

    private final Map<BinaryRow, Map<Integer, LookupLevels<KeyValue>>> tableView;

    private final CoreOptions options;

    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;

    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;

    private final LookupStoreFactory lookupStoreFactory;

    private final int startLevel;

    private IOManager ioManager;

    @Nullable private Cache<String, LookupFile> lookupFileCache;

    private final RowType rowType;
    private final RowType partitionType;

    // --- icestream monkey patch: needed to wire the remote-sst downloader on the read path ---
    private final FileIO fileIO;

    @Nullable private Filter<InternalRow> cacheRowFilter;

    public LocalTableQuery(FileStoreTable table) {
        this.options = table.coreOptions();
        this.tableView = new HashMap<>();
        FileStore<?> tableStore = table.store();
        if (!(tableStore instanceof KeyValueFileStore)) {
            throw new UnsupportedOperationException(
                    "Table Query only supports table with primary key.");
        }
        KeyValueFileStore store = (KeyValueFileStore) tableStore;

        this.readerFactoryBuilder = store.newReaderFactoryBuilder();
        this.rowType = table.schema().logicalRowType();
        this.partitionType = table.schema().logicalPartitionType();
        this.fileIO = table.fileIO(); // icestream monkey patch
        RowType keyType = readerFactoryBuilder.keyType();
        this.keyComparatorSupplier = new KeyComparatorSupplier(readerFactoryBuilder.keyType());
        this.lookupStoreFactory =
                LookupStoreFactory.create(
                        options,
                        new CacheManager(
                                options.lookupCacheMaxMemory(),
                                options.lookupCacheHighPrioPoolRatio()),
                        new RowCompactedSerializer(keyType).createSliceComparator());
        startLevel = options.needLookup() ? 1 : 0;
    }

    public void refreshFiles(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> beforeFiles,
            List<DataFileMeta> dataFiles) {
        LookupLevels<KeyValue> lookupLevels =
                tableView.computeIfAbsent(partition, k -> new HashMap<>()).get(bucket);
        if (lookupLevels == null) {
            // Initial phase: ignore beforeFiles as they represent deletions from previous state
            newLookupLevels(partition, bucket, dataFiles);
        } else {
            lookupLevels.getLevels().update(beforeFiles, dataFiles);
        }
    }

    /**
     * icestream observability: counts cold per-(partition,bucket) lookup builds — i.e. cache misses
     * where this query has to materialize a bucket's lookup levels (and download/rebuild its lookup
     * ssts) rather than reuse the warm {@code tableView} entry. The whole point of the long-lived
     * autonomous job is that this stays flat across conversions for a bucket already loaded; a test
     * asserts exactly that ({@code LookupCacheWarmthTest}).
     */
    public static final java.util.concurrent.atomic.AtomicLong COLD_BUCKET_LOADS =
            new java.util.concurrent.atomic.AtomicLong();

    private void newLookupLevels(BinaryRow partition, int bucket, List<DataFileMeta> dataFiles) {
        COLD_BUCKET_LOADS.incrementAndGet();
        Levels levels = new Levels(keyComparatorSupplier.get(), dataFiles, options.numLevels());
        // TODO pass DeletionVector factory
        KeyValueFileReaderFactory factory =
                readerFactoryBuilder.build(partition, bucket, DeletionVector.emptyFactory());
        Options options = this.options.toConfiguration();
        if (lookupFileCache == null) {
            lookupFileCache =
                    LookupFile.createCache(
                            options.get(CoreOptions.LOOKUP_CACHE_FILE_RETENTION),
                            options.get(CoreOptions.LOOKUP_CACHE_MAX_DISK_SIZE));
        }

        RowType readValueType = readerFactoryBuilder.readValueType();
        LookupLevels<KeyValue> lookupLevels =
                new LookupLevels<>(
                        schemaId -> readValueType,
                        0L,
                        levels,
                        keyComparatorSupplier.get(),
                        readerFactoryBuilder.keyType(),
                        PersistValueProcessor.factory(readValueType),
                        LookupSerializerFactory.INSTANCE.get(),
                        file -> {
                            RecordReader<KeyValue> reader = factory.createRecordReader(file);
                            if (cacheRowFilter != null) {
                                reader =
                                        reader.filter(
                                                keyValue -> cacheRowFilter.test(keyValue.value()));
                            }
                            return reader;
                        },
                        file ->
                                Preconditions.checkNotNull(ioManager, "IOManager is required.")
                                        .createChannel(
                                                localFilePrefix(
                                                        partitionType, partition, bucket, file))
                                        .getPathFile(),
                        lookupStoreFactory,
                        bfGenerator(options),
                        lookupFileCache);

        // --- icestream monkey patch ---
        // Stock LocalTableQuery never wires a RemoteFileDownloader, so the read path can't download
        // the remote ssts the writer persisted (it rebuilds from the data file instead). Wire one when
        // remote files are enabled; the manager self-registers on the LookupLevels via its constructor.
        if (this.options.lookupRemoteFileEnabled()) {
            new RemoteLookupFileManager<>(
                    fileIO,
                    factory.pathFactory(),
                    lookupLevels,
                    this.options.lookupRemoteLevelThreshold());
        }

        tableView.computeIfAbsent(partition, k -> new HashMap<>()).put(bucket, lookupLevels);
    }

    /** TODO remove synchronized and supports multiple thread to lookup. */
    @Nullable
    @Override
    public synchronized InternalRow lookup(BinaryRow partition, int bucket, InternalRow key)
            throws IOException {
        Map<Integer, LookupLevels<KeyValue>> buckets = tableView.get(partition);
        if (buckets == null || buckets.isEmpty()) {
            return null;
        }
        LookupLevels<KeyValue> lookupLevels = buckets.get(bucket);
        if (lookupLevels == null) {
            return null;
        }

        KeyValue kv = lookupLevels.lookup(key, startLevel);
        if (kv == null || kv.valueKind().isRetract()) {
            return null;
        } else {
            return kv.value();
        }
    }

    @Override
    public LocalTableQuery withValueProjection(int[] projection) {
        this.readerFactoryBuilder.withReadValueType(rowType.project(projection));
        return this;
    }

    public LocalTableQuery withIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
        return this;
    }

    public LocalTableQuery withCacheRowFilter(Filter<InternalRow> cacheRowFilter) {
        this.cacheRowFilter = cacheRowFilter;
        return this;
    }

    @Override
    public InternalRowSerializer createValueSerializer() {
        return InternalSerializers.create(readerFactoryBuilder.readValueType());
    }

    @Override
    public void close() throws IOException {
        for (Map.Entry<BinaryRow, Map<Integer, LookupLevels<KeyValue>>> buckets :
                tableView.entrySet()) {
            for (Map.Entry<Integer, LookupLevels<KeyValue>> bucket :
                    buckets.getValue().entrySet()) {
                bucket.getValue().close();
            }
        }
        if (lookupFileCache != null) {
            lookupFileCache.invalidateAll();
        }
        tableView.clear();
    }
}
