package io.github.jordepic.icestream.it;

import java.time.Duration;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Pushes JSON records of the form {@code {"id":..,"name":..,"ts":..}} to Kafka. Key cardinality is
 * bounded by {@link #keyCardinality} so Flink's upsert sink emits frequent eq-deletesByDataFilePath — exactly
 * what icestream needs to convert.
 *
 * <p>Stops when the deadline elapses or {@link #stop()} is called. Wraps {@link KafkaProducer}'s
 * lifecycle.
 */
final class KafkaJsonProducer implements Runnable {

    private final String bootstrapServers;
    private final String topic;
    private final long keyCardinality;
    private final Duration runtime;
    private final Duration sendInterval;
    private final long randomSeed;

    private final AtomicLong sent = new AtomicLong();
    private volatile boolean stopped = false;

    KafkaJsonProducer(
            String bootstrapServers,
            String topic,
            long keyCardinality,
            Duration runtime,
            Duration sendInterval,
            long randomSeed) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.keyCardinality = keyCardinality;
        this.runtime = runtime;
        this.sendInterval = sendInterval;
        this.randomSeed = randomSeed;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        Random rng = new Random(randomSeed);
        long deadline = System.nanoTime() + runtime.toNanos();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            while (!stopped && System.nanoTime() < deadline) {
                long id = (rng.nextLong() % keyCardinality + keyCardinality) % keyCardinality;
                long ts = System.currentTimeMillis();
                String name = "row-" + rng.nextInt(1_000_000);
                String json = "{\"id\":" + id + ",\"name\":\"" + name + "\",\"ts\":" + ts + "}";
                producer.send(new ProducerRecord<>(topic, String.valueOf(id), json));
                sent.incrementAndGet();
                sleep(sendInterval);
            }
            producer.flush();
        }
    }

    void stop() {
        stopped = true;
    }

    long sent() {
        return sent.get();
    }

    private static void sleep(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
