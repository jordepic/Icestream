package io.github.jordepic.icestream.sparkcassandra.master;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.jordepic.icestream.schema.IcestreamProperties;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.github.jordepic.icestream.master.MasterLoop;
import io.github.jordepic.icestream.master.RunProcessor;

class MasterLoopTest {

    private FakeCatalog catalog;
    private MasterLoop loop;
    private Thread loopThread;

    @BeforeEach
    void setup() {
        catalog = new FakeCatalog();
    }

    @AfterEach
    void teardown() throws InterruptedException {
        if (loop != null) {
            loop.stop();
        }
        if (loopThread != null) {
            loopThread.join(TimeUnit.SECONDS.toMillis(5));
        }
    }

    @Test
    void respectsMaxConcurrentTasks() throws InterruptedException {
        int max = 2;
        for (int i = 0; i < 5; i++) {
            catalog.addIcestreamTable(TableIdentifier.of("db", "t" + i));
        }
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxObserved = new AtomicInteger();
        RecordingProcessor processor = new RecordingProcessor(id -> {
            int n = inFlight.incrementAndGet();
            maxObserved.updateAndGet(prev -> Math.max(prev, n));
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            inFlight.decrementAndGet();
            return true;
        });

        startLoop(processor, Duration.ofMillis(20), Duration.ZERO, max);

        awaitTrue(() -> inFlight.get() == max, Duration.ofSeconds(3));
        Thread.sleep(150);
        assertThat(inFlight.get()).isEqualTo(max);
        assertThat(maxObserved.get()).isEqualTo(max);
        release.countDown();
    }

    @Test
    void noDuplicateTaskForSameTableInFlight() throws InterruptedException {
        TableIdentifier id = TableIdentifier.of("db", "single");
        catalog.addIcestreamTable(id);
        CountDownLatch release = new CountDownLatch(1);
        RecordingProcessor processor = new RecordingProcessor(t -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return true;
        });

        startLoop(processor, Duration.ofMillis(20), Duration.ZERO, 1);

        awaitTrue(() -> processor.totalCalls() == 1, Duration.ofSeconds(2));
        Thread.sleep(200);
        assertThat(processor.totalCalls()).isEqualTo(1);
        release.countDown();
    }

    @Test
    void freshlyCompletedTableYieldsToOthers_noStarvation() throws InterruptedException {
        List<TableIdentifier> tables = List.of(
                TableIdentifier.of("db", "a"),
                TableIdentifier.of("db", "b"),
                TableIdentifier.of("db", "c"));
        tables.forEach(catalog::addIcestreamTable);
        RecordingProcessor processor = new RecordingProcessor(id -> true);

        startLoop(processor, Duration.ofMillis(50), Duration.ZERO, 1);

        awaitTrue(() -> tables.stream().allMatch(t -> processor.callsFor(t) >= 2), Duration.ofSeconds(3));
        for (TableIdentifier id : tables) {
            assertThat(processor.callsFor(id)).isGreaterThan(0);
        }
    }

    @Test
    void idleTableSitsOutForBackoff() throws InterruptedException {
        TableIdentifier id = TableIdentifier.of("db", "idle");
        catalog.addIcestreamTable(id);
        RecordingProcessor processor = new RecordingProcessor(t -> false);

        startLoop(processor, Duration.ofMillis(20), Duration.ofSeconds(30), 1);

        awaitTrue(() -> processor.totalCalls() == 1, Duration.ofSeconds(2));
        Thread.sleep(300);
        assertThat(processor.totalCalls()).isEqualTo(1);
    }

    @Test
    void taskFailureRequeuesOnNextPoll() throws InterruptedException {
        TableIdentifier id = TableIdentifier.of("db", "flaky");
        catalog.addIcestreamTable(id);
        AtomicInteger calls = new AtomicInteger();
        RecordingProcessor processor = new RecordingProcessor(t -> {
            int n = calls.incrementAndGet();
            if (n == 1) {
                throw new RuntimeException("boom");
            }
            return true;
        });

        startLoop(processor, Duration.ofMillis(20), Duration.ZERO, 1);

        awaitTrue(() -> calls.get() >= 2, Duration.ofSeconds(3));
    }

    @Test
    void tableMissingPrimaryKeysFiltered() throws InterruptedException {
        TableIdentifier hasPk = TableIdentifier.of("db", "yes");
        TableIdentifier noPk = TableIdentifier.of("db", "no");
        catalog.addIcestreamTable(hasPk);
        catalog.addPlainTable(noPk);
        RecordingProcessor processor = new RecordingProcessor(t -> true);

        startLoop(processor, Duration.ofMillis(20), Duration.ZERO, 1);

        awaitTrue(() -> processor.callsFor(hasPk) >= 2, Duration.ofSeconds(2));
        assertThat(processor.callsFor(noPk)).isZero();
    }

    @Test
    void iteratesAllNamespacesRecursively() throws InterruptedException {
        TableIdentifier inA = TableIdentifier.of(Namespace.of("a"), "t");
        TableIdentifier inAB = TableIdentifier.of(Namespace.of("a", "b"), "t");
        TableIdentifier inC = TableIdentifier.of(Namespace.of("c"), "t");
        catalog.addNamespace(Namespace.empty(), Namespace.of("a"), Namespace.of("c"));
        catalog.addNamespace(Namespace.of("a"), Namespace.of("a", "b"));
        catalog.addIcestreamTable(inA);
        catalog.addIcestreamTable(inAB);
        catalog.addIcestreamTable(inC);
        RecordingProcessor processor = new RecordingProcessor(t -> true);

        startLoop(processor, Duration.ofMillis(20), Duration.ZERO, 2);

        awaitTrue(
                () -> processor.callsFor(inA) > 0
                        && processor.callsFor(inAB) > 0
                        && processor.callsFor(inC) > 0,
                Duration.ofSeconds(3));
    }

    @Test
    void stopInterruptsLoop() throws InterruptedException {
        catalog.addIcestreamTable(TableIdentifier.of("db", "t"));
        RecordingProcessor processor = new RecordingProcessor(t -> true);

        startLoop(processor, Duration.ofSeconds(60), Duration.ZERO, 1);
        Thread.sleep(50);
        loop.stop();

        loopThread.join(TimeUnit.SECONDS.toMillis(2));
        assertThat(loopThread.isAlive()).isFalse();
    }

    private void startLoop(
            RunProcessor processor, Duration pollInterval, Duration idleBackoff, int maxConcurrentTasks) {
        loop = new MasterLoop(catalog, processor, pollInterval, idleBackoff, maxConcurrentTasks);
        loopThread = new Thread(loop::run, "masterloop-test");
        loopThread.setDaemon(true);
        loopThread.start();
    }

    private static void awaitTrue(BooleanSupplier cond, Duration timeout) throws InterruptedException {
        long deadlineNs = System.nanoTime() + timeout.toNanos();
        while (!cond.getAsBoolean()) {
            if (System.nanoTime() >= deadlineNs) {
                throw new AssertionError("Timed out waiting for condition");
            }
            Thread.sleep(10);
        }
    }

    private static final class RecordingProcessor implements RunProcessor {

        private final Function<TableIdentifier, Boolean> behavior;
        private final List<TableIdentifier> calls = new CopyOnWriteArrayList<>();

        RecordingProcessor(Function<TableIdentifier, Boolean> behavior) {
            this.behavior = behavior;
        }

        @Override
        public boolean processNextRun(TableIdentifier id, Table table) {
            calls.add(id);
            return behavior.apply(id);
        }

        int totalCalls() {
            return calls.size();
        }

        long callsFor(TableIdentifier id) {
            return calls.stream().filter(id::equals).count();
        }
    }

    private static final class FakeCatalog implements Catalog, SupportsNamespaces {

        private final Map<TableIdentifier, Map<String, String>> tables = new ConcurrentHashMap<>();
        private final Map<Namespace, List<Namespace>> namespaceChildren = new ConcurrentHashMap<>();

        void addIcestreamTable(TableIdentifier id) {
            registerNamespacePath(id.namespace());
            Map<String, String> props = new HashMap<>();
            props.put(IcestreamProperties.PRIMARY_KEYS, "id");
            tables.put(id, props);
        }

        void addPlainTable(TableIdentifier id) {
            registerNamespacePath(id.namespace());
            tables.put(id, Map.of());
        }

        void addNamespace(Namespace parent, Namespace... children) {
            for (Namespace child : children) {
                addChildIfAbsent(parent, child);
            }
        }

        private void registerNamespacePath(Namespace ns) {
            String[] levels = ns.levels();
            Namespace parent = Namespace.empty();
            for (int i = 0; i < levels.length; i++) {
                Namespace child = Namespace.of(Arrays.copyOf(levels, i + 1));
                addChildIfAbsent(parent, child);
                parent = child;
            }
        }

        private void addChildIfAbsent(Namespace parent, Namespace child) {
            List<Namespace> children = namespaceChildren.computeIfAbsent(parent, k -> new CopyOnWriteArrayList<>());
            if (!children.contains(child)) {
                children.add(child);
            }
        }

        @Override
        public List<TableIdentifier> listTables(Namespace namespace) {
            List<TableIdentifier> out = new ArrayList<>();
            for (TableIdentifier id : tables.keySet()) {
                if (id.namespace().equals(namespace)) {
                    out.add(id);
                }
            }
            return out;
        }

        @Override
        public Table loadTable(TableIdentifier id) {
            Map<String, String> props = tables.get(id);
            if (props == null) {
                throw new NoSuchTableException("No such table: %s", id);
            }
            return fakeTable(id, props);
        }

        @Override
        public boolean dropTable(TableIdentifier id, boolean purge) {
            return tables.remove(id) != null;
        }

        @Override
        public void renameTable(TableIdentifier from, TableIdentifier to) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Namespace> listNamespaces(Namespace namespace) {
            return new ArrayList<>(namespaceChildren.getOrDefault(namespace, List.of()));
        }

        @Override
        public void createNamespace(Namespace namespace, Map<String, String> metadata) {}

        @Override
        public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
            return Map.of();
        }

        @Override
        public boolean dropNamespace(Namespace namespace) {
            return false;
        }

        @Override
        public boolean setProperties(Namespace namespace, Map<String, String> properties) {
            return false;
        }

        @Override
        public boolean removeProperties(Namespace namespace, Set<String> properties) {
            return false;
        }
    }

    private static Table fakeTable(TableIdentifier id, Map<String, String> properties) {
        InvocationHandler handler = (proxy, method, args) -> switch (method.getName()) {
            case "properties" -> properties;
            case "name" -> id.toString();
            case "refresh" -> null;
            case "toString" -> "FakeTable[" + id + "]";
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == args[0];
            default -> throw new UnsupportedOperationException("FakeTable does not support " + method);
        };
        return (Table) Proxy.newProxyInstance(
                MasterLoopTest.class.getClassLoader(), new Class<?>[] {Table.class}, handler);
    }
}
