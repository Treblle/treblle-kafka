package com.treblle.kafka.core;

import java.net.InetAddress;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

/**
 * Host and runtime facts that never change during a JVM's lifetime.
 *
 * <p>Resolved lazily on first use, which always happens on the dispatcher's worker thread - never
 * on a Kafka I/O thread or an application thread. {@code InetAddress.getLocalHost()} can block on
 * a misconfigured DNS setup, so it must stay off the hot path.
 */
public final class RuntimeInfo {

    /** Treblle's placeholder for an IP that could not be determined. */
    public static final String UNKNOWN_IP = "bogon";

    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);

    private static volatile String cachedIp;

    private RuntimeInfo() {
    }

    /** The host's IP address, or {@value #UNKNOWN_IP} if it cannot be resolved. */
    public static String serverIp() {
        String resolved = cachedIp;
        if (resolved == null) {
            resolved = resolveIp();
            cachedIp = resolved;
        }
        return resolved;
    }

    private static String resolveIp() {
        try {
            InetAddress local = InetAddress.getLocalHost();
            String address = local.getHostAddress();
            if (address == null || address.isEmpty() || local.isLoopbackAddress()) {
                return UNKNOWN_IP;
            }
            return address;
        } catch (Exception e) {
            return UNKNOWN_IP;
        }
    }

    public static String timezone() {
        try {
            String id = TimeZone.getDefault().getID();
            return id == null || id.isEmpty() ? "UTC" : id;
        } catch (Exception e) {
            return "UTC";
        }
    }

    public static String javaVersion() {
        return property("java.version");
    }

    public static String osName() {
        return property("os.name");
    }

    public static String osVersion() {
        return property("os.version");
    }

    public static String osArchitecture() {
        return property("os.arch");
    }

    /** Formats epoch millis as Treblle's {@code Y-m-d H:i:s}, always in UTC. */
    public static String formatTimestamp(long epochMillis) {
        long millis = epochMillis > 0 ? epochMillis : System.currentTimeMillis();
        return TIMESTAMP_FORMAT.format(Instant.ofEpochMilli(millis));
    }

    private static String property(String key) {
        try {
            String value = System.getProperty(key);
            return value == null || value.isEmpty() ? null : value;
        } catch (SecurityException e) {
            return null;
        }
    }
}
