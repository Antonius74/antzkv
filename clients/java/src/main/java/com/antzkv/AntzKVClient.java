package com.antzkv;

import java.io.*;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Thread-safe native TCP client for antzkv key-value store.
 *
 * <p>Usage example:
 * <pre>{@code
 * try (AntzKVClient client = new AntzKVClient("127.0.0.1", 6379)) {
 *     client.set("name", "Alice");
 *     String value = client.get("name"); // "Alice"
 *     client.delete("name");
 * }
 * }</pre>
 */
public class AntzKVClient implements AutoCloseable {
    private final String host;
    private final int port;
    private Socket socket;
    private BufferedWriter writer;
    private BufferedReader reader;
    private final Object lock = new Object();

    public AntzKVClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /** Open TCP connection. */
    public void connect() throws IOException {
        synchronized (lock) {
            if (socket != null && !socket.isClosed()) return;
            socket = new Socket(host, port);
            socket.setTcpNoDelay(true);
            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"));
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));
        }
    }

    /** Close connection. */
    @Override
    public void close() {
        synchronized (lock) {
            try {
                if (writer != null) writer.close();
            } catch (IOException ignored) {}
            try {
                if (reader != null) reader.close();
            } catch (IOException ignored) {}
            try {
                if (socket != null) socket.close();
            } catch (IOException ignored) {}
            socket = null;
            writer = null;
            reader = null;
        }
    }

    private String sendAndReceive(String command) throws IOException {
        synchronized (lock) {
            if (socket == null || socket.isClosed()) {
                throw new IOException("Not connected. Call connect() first.");
            }
            writer.write(command);
            writer.write('\n');
            writer.flush();
            String line = reader.readLine();
            if (line == null) {
                throw new IOException("Server closed connection unexpectedly.");
            }
            return line;
        }
    }

    /** Store value under key. Returns true on success. */
    public boolean set(String key, String value) throws IOException {
        return "OK".equals(sendAndReceive("SET " + key + " " + value));
    }

    /** Retrieve value. Returns null if key does not exist. */
    public String get(String key) throws IOException {
        String r = sendAndReceive("GET " + key);
        return "(nil)".equals(r) ? null : r;
    }

    /** Delete one or more keys. Returns number removed. */
    public int delete(String... keys) throws IOException {
        if (keys.length == 0) return 0;
        StringBuilder sb = new StringBuilder("DEL");
        for (String k : keys) sb.append(' ').append(k);
        String r = sendAndReceive(sb.toString());
        try { return Integer.parseInt(r); } catch (NumberFormatException e) { return 0; }
    }

    /** Check existence. Returns count of existing keys. */
    public int exists(String... keys) throws IOException {
        if (keys.length == 0) return 0;
        StringBuilder sb = new StringBuilder("EXISTS");
        for (String k : keys) sb.append(' ').append(k);
        String r = sendAndReceive(sb.toString());
        try { return Integer.parseInt(r); } catch (NumberFormatException e) { return 0; }
    }

    /** Return all keys. */
    public List<String> keys() throws IOException {
        String r = sendAndReceive("KEYS");
        if ("(empty)".equals(r)) return Arrays.asList();
        return Arrays.asList(r.split(" "));
    }

    /** Persist to disk. */
    public boolean save() throws IOException {
        return "OK".equals(sendAndReceive("SAVE"));
    }

    /** Healthcheck. */
    public boolean ping() throws IOException {
        return "PONG".equals(sendAndReceive("PING"));
    }

    /** Graceful close. */
    public boolean quit() throws IOException {
        return "OK".equals(sendAndReceive("QUIT"));
    }

    /** Simple connection pool. */
    public static class Pool implements AutoCloseable {
        private final BlockingQueue<AntzKVClient> pool;
        private final String host;
        private final int port;

        public Pool(String host, int port, int size) throws IOException {
            this.host = host;
            this.port = port;
            this.pool = new ArrayBlockingQueue<>(size);
            for (int i = 0; i < size; i++) {
                AntzKVClient c = new AntzKVClient(host, port);
                c.connect();
                pool.offer(c);
            }
        }

        public AntzKVClient acquire() throws InterruptedException {
            return pool.take();
        }

        public void release(AntzKVClient client) {
            pool.offer(client);
        }

        @Override
        public void close() {
            for (AntzKVClient c : pool) {
                c.close();
            }
            pool.clear();
        }
    }
}
