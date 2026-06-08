package io.github.jordepic.icestream.flinkpaimon.channel;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.jordepic.icestream.converter.TaskOutputs;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP server for the conversion channel — the single transport between the driver and the standing
 * job's operators. The converter hosts one of these; the job's source/collector (via
 * {@link RemoteConversionChannelClient}, on a TaskManager or in an in-JVM MiniCluster over loopback)
 * dial back here to pull conversion units and return results. It just bridges those calls to the
 * in-JVM {@link InProcessConversionChannel} that the driver's {@code create()} submits to — so the
 * channel state stays in one place.
 *
 * <p>Endpoints (the operators dial out to the driver's known URL — no TM discovery):
 * <ul>
 *   <li>{@code /channel/poll?jobKey&timeoutMs} — long-poll → 204 on timeout, else serialized request.
 *   <li>{@code /channel/inflight?jobKey&conversionId} — mark in-flight under the source's lock → 200.
 *   <li>{@code /channel/complete?jobKey} — body: serialized {@code List<TaskOutputs>} → 200.
 * </ul>
 */
public final class ConversionChannelServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConversionChannelServer.class);

    private final HttpServer http;

    public ConversionChannelServer(int port) throws IOException {
        this.http = HttpServer.create(new InetSocketAddress(port), 0);
        http.createContext("/channel/poll", exchange -> safe(exchange, this::poll));
        http.createContext("/channel/inflight", exchange -> safe(exchange, this::inflight));
        http.createContext("/channel/complete", exchange -> safe(exchange, this::complete));
        http.setExecutor(Executors.newCachedThreadPool());
    }

    public ConversionChannelServer start() {
        http.start();
        log.info("Icestream conversion channel server listening on port {}", http.getAddress().getPort());
        return this;
    }

    public int port() {
        return http.getAddress().getPort();
    }

    private void poll(HttpExchange exchange) throws Exception {
        Map<String, String> q = query(exchange);
        ConversionRequest request =
                InProcessConversionChannel.poll(q.get("jobKey"), Long.parseLong(q.get("timeoutMs")));
        if (request == null) {
            exchange.sendResponseHeaders(204, -1); // poll timed out — no request available
            return;
        }
        byte[] body = ConversionChannelCodec.toBytes(request);
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    private void inflight(HttpExchange exchange) throws Exception {
        Map<String, String> q = query(exchange);
        InProcessConversionChannel.markInFlight(q.get("jobKey"), Long.parseLong(q.get("conversionId")));
        exchange.sendResponseHeaders(200, -1);
    }

    private void complete(HttpExchange exchange) throws Exception {
        Map<String, String> q = query(exchange);
        @SuppressWarnings("unchecked")
        List<TaskOutputs> outputs =
                (List<TaskOutputs>) ConversionChannelCodec.fromBytes(exchange.getRequestBody().readAllBytes(), List.class);
        InProcessConversionChannel.complete(q.get("jobKey"), outputs);
        exchange.sendResponseHeaders(200, -1);
    }

    private static Map<String, String> query(HttpExchange exchange) {
        Map<String, String> params = new HashMap<>();
        String raw = exchange.getRequestURI().getRawQuery();
        if (raw == null) {
            return params;
        }
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0) {
                params.put(
                        URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8),
                        URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
            }
        }
        return params;
    }

    private void safe(HttpExchange exchange, ExchangeHandler handler) throws IOException {
        try (exchange) {
            try {
                handler.apply(exchange);
            } catch (Exception e) {
                log.warn("channel handler failed for {}", exchange.getRequestURI(), e);
                byte[] msg = String.valueOf(e).getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(500, msg.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(msg);
                }
            }
        }
    }

    private interface ExchangeHandler {
        void apply(HttpExchange exchange) throws Exception;
    }

    @Override
    public void close() {
        http.stop(0);
    }
}
