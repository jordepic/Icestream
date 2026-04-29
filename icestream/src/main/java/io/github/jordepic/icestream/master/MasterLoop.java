package io.github.jordepic.icestream.master;

import io.github.jordepic.icestream.schema.IcestreamProperties;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Two-stage scheduler: a single polling thread enqueues icestream tables it sees, while a fixed
 * worker pool drains them and asks the {@link RunProcessor} to process exactly one run.
 *
 * <p>Fairness: the {@code active} set holds every table currently queued or in-flight. A table can
 * only re-enter the queue via the next polling tick, which appends it to the back — freshly
 * completed tables therefore yield to others. Idle tables (no work last time) sit out until their
 * cooldown elapses, so empty tables don't dominate the polling stream.
 *
 * <p>All worker and polling threads are daemons; {@link #run} blocks the caller on the polling
 * thread's join so the JVM stays alive. Ctrl+c (SIGINT) triggers JVM shutdown hooks; if a hook
 * calls {@link #stop()} the polling thread is interrupted and {@code run} returns. Workers are
 * cleaned up in {@code run}'s finally block.
 */
public final class MasterLoop {

    private static final Logger log = LoggerFactory.getLogger(MasterLoop.class);

    private final Catalog catalog;
    private final RunProcessor processor;
    private final Duration pollInterval;
    private final Duration idleBackoff;
    private final int maxConcurrentTasks;

    private final LinkedBlockingQueue<TableIdentifier> queue = new LinkedBlockingQueue<>();
    private final Set<TableIdentifier> active = ConcurrentHashMap.newKeySet();
    private final Map<TableIdentifier, Instant> noWorkUntil = new ConcurrentHashMap<>();

    private volatile Thread pollingThread;

    public MasterLoop(
            Catalog catalog,
            RunProcessor processor,
            Duration pollInterval,
            Duration idleBackoff,
            int maxConcurrentTasks) {
        this.catalog = catalog;
        this.processor = processor;
        this.pollInterval = pollInterval;
        this.idleBackoff = idleBackoff;
        this.maxConcurrentTasks = maxConcurrentTasks;
    }

    public void run() {
        ExecutorService workerPool =
                Executors.newFixedThreadPool(maxConcurrentTasks, daemonFactory("icestream-worker"));
        Thread poller = new Thread(this::pollLoop, "icestream-poller");
        poller.setDaemon(true);
        pollingThread = poller;
        for (int i = 0; i < maxConcurrentTasks; i++) {
            workerPool.submit(this::workerLoop);
        }
        poller.start();
        try {
            poller.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            poller.interrupt();
            workerPool.shutdownNow();
            try {
                workerPool.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void stop() {
        Thread poller = pollingThread;
        if (poller != null) {
            poller.interrupt();
        }
    }

    private void pollLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                pollOnce();
                Thread.sleep(pollInterval.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.error("Polling sweep failed; retrying after pollInterval", e);
                if (!sleepInterruptibly(pollInterval)) {
                    return;
                }
            }
        }
    }

    private void pollOnce() {
        Instant now = Instant.now();
        for (TableIdentifier id : enumerateIcestreamTables()) {
            Instant cooldown = noWorkUntil.get(id);
            if (cooldown != null && cooldown.isAfter(now)) {
                continue;
            }
            if (active.add(id)) {
                queue.offer(id);
            }
        }
    }

    private void workerLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            TableIdentifier id;
            try {
                id = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            try {
                Table table = catalog.loadTable(id);
                table.refresh();
                boolean didWork = processor.processNextRun(id, table);
                if (didWork) {
                    noWorkUntil.remove(id);
                } else {
                    noWorkUntil.put(id, Instant.now().plus(idleBackoff));
                }
            } catch (NoSuchTableException e) {
                log.info("Table {} no longer exists; skipping", id);
            } catch (Exception e) {
                log.warn("Processing table {} failed; will retry on next sweep", id, e);
            } finally {
                active.remove(id);
            }
        }
    }

    private List<TableIdentifier> enumerateIcestreamTables() {
        List<TableIdentifier> out = new ArrayList<>();
        for (Namespace ns : walkNamespaces()) {
            try {
                for (TableIdentifier id : catalog.listTables(ns)) {
                    if (isIcestreamTable(id)) {
                        out.add(id);
                    }
                }
            } catch (Exception e) {
                log.warn("Failed to list tables in namespace {}; skipping", ns, e);
            }
        }
        return out;
    }

    private boolean isIcestreamTable(TableIdentifier id) {
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

    private List<Namespace> walkNamespaces() {
        if (!(catalog instanceof SupportsNamespaces sn)) {
            return List.of(Namespace.empty());
        }
        Deque<Namespace> stack = new ArrayDeque<>();
        stack.push(Namespace.empty());
        List<Namespace> out = new ArrayList<>();
        while (!stack.isEmpty()) {
            Namespace cur = stack.pop();
            out.add(cur);
            try {
                for (Namespace child : sn.listNamespaces(cur)) {
                    stack.push(child);
                }
            } catch (Exception e) {
                log.warn("Failed to list children of {}; skipping subtree", cur, e);
            }
        }
        return out;
    }

    private static boolean sleepInterruptibly(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static ThreadFactory daemonFactory(String prefix) {
        AtomicInteger counter = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }
}
