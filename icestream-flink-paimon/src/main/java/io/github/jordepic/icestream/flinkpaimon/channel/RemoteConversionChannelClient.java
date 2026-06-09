package io.github.jordepic.icestream.flinkpaimon.channel;

import io.github.jordepic.icestream.converter.TaskOutputs;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The conversion channel's HTTP client — the only transport. The standing job's source and writer
 * reach the driver's channel state over HTTP whether they run in an in-JVM MiniCluster (loopback) or
 * on a TaskManager in another JVM. The operators dial OUT to the driver's known {@code baseUrl}
 * (chosen so we never need to discover the TaskManager's address) and hit the channel endpoints
 * exposed by {@link ConversionChannelServer}:
 * <ul>
 *   <li>{@code POST /channel/poll?jobKey&timeoutMs} → 204 on timeout, else 200 + serialized request.
 *   <li>{@code POST /channel/inflight?jobKey&conversionId} → 200.
 *   <li>{@code POST /channel/complete?jobKey} (body: serialized {@code ArrayList<TaskOutputs>}) → 200.
 * </ul>
 *
 * <p><b>Resilience.</b> The JDK {@code HttpClient} pools connections and the JDK {@code HttpServer}
 * closes idle ones, so a reused-but-closed connection surfaces as a {@code Connection reset}. Each
 * call {@linkplain #send retries once}, dropping the pooled connection first so the retry uses a fresh
 * one — that covers the stale-connection case (the only one seen in practice). A genuine failure is
 * then handled at a higher level, never propagating out of an operator (which would restart the whole
 * standing job and lose the warm cache): {@link #poll} swallows it and returns {@code null} (the
 * source re-polls), and a {@link #markInFlight}/{@link #complete} failure leaves the conversion to
 * time out at the master, which re-dispatches it (idempotent).
 *
 * <p>{@code baseUrl} is the only serialized state; the {@link HttpClient} is built lazily per JVM
 * (transient) since this object is shipped to each TaskManager as an operator field.
 */
public final class RemoteConversionChannelClient implements java.io.Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RemoteConversionChannelClient.class);

    private final String baseUrl;
    private transient HttpClient http;

    public RemoteConversionChannelClient(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    }

    private HttpClient http() {
        if (http == null) {
            http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        }
        return http;
    }

    public ConversionRequest poll(String jobKey, long timeoutMs) throws InterruptedException {
        HttpRequest req = HttpRequest.newBuilder(
                        URI.create(baseUrl + "/channel/poll?jobKey=" + enc(jobKey) + "&timeoutMs=" + timeoutMs))
                .timeout(Duration.ofMillis(timeoutMs + 10_000))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<byte[]> response;
        try {
            response = send(req);
        } catch (IOException e) {
            // Never crash the source on a transport error — re-poll. A request taken server-side but
            // lost in transit just leaves its future to time out, and the master re-dispatches it.
            LOG.warn("channel poll failed ({}), re-polling", e.toString());
            return null;
        }
        if (response.statusCode() == 204) {
            return null; // server-side poll timed out — no request available, re-poll
        }
        require200(response, "/channel/poll");
        return ConversionChannelCodec.fromBytes(response.body(), ConversionRequest.class);
    }

    public void markInFlight(String jobKey, long conversionId) {
        HttpRequest req = HttpRequest.newBuilder(
                        URI.create(baseUrl + "/channel/inflight?jobKey=" + enc(jobKey) + "&conversionId=" + conversionId))
                .timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        sendOrThrow(req, "/channel/inflight");
    }

    public void complete(String jobKey, List<TaskOutputs> outputs) {
        HttpRequest req = HttpRequest.newBuilder(
                        URI.create(baseUrl + "/channel/complete?jobKey=" + enc(jobKey)))
                .timeout(Duration.ofSeconds(60))
                .POST(HttpRequest.BodyPublishers.ofByteArray(ConversionChannelCodec.toBytes(new ArrayList<>(outputs))))
                .build();
        sendOrThrow(req, "/channel/complete");
    }

    private void sendOrThrow(HttpRequest req, String path) {
        try {
            require200(send(req), path);
        } catch (IOException e) {
            throw new RuntimeException("channel " + path + " failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted on " + path, e);
        }
    }

    /**
     * Send the request, retrying <b>once</b> on a transient {@link IOException}: the first failure is
     * almost always a stale pooled connection the server already closed, so we drop the pool
     * ({@code http = null}) and the retry builds a fresh client/connection.
     */
    private HttpResponse<byte[]> send(HttpRequest req) throws IOException, InterruptedException {
        try {
            return http().send(req, HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException firstFailure) {
            http = null; // shed the (likely stale) pooled connection; http() rebuilds fresh
            return http().send(req, HttpResponse.BodyHandlers.ofByteArray());
        }
    }

    private static void require200(HttpResponse<byte[]> response, String path) {
        if (response.statusCode() != 200) {
            throw new IllegalStateException("channel " + path + " failed (" + response.statusCode() + "): "
                    + new String(response.body(), StandardCharsets.UTF_8));
        }
    }

    private static String enc(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
