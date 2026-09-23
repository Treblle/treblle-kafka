package com.treblle.kafka.util;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Debug output for the SDK.
 *
 * <p>Silent unless {@code debug} is enabled. Uses {@code java.util.logging} so the host
 * application's existing logging configuration (including SLF4J/Log4j JUL bridges) picks the
 * output up without the SDK taking a logging dependency.
 */
public final class DebugLogger {

    private static final Logger LOGGER = Logger.getLogger("com.treblle.kafka");

    private final boolean enabled;

    public DebugLogger(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return enabled;
    }

    /** Logs a normal SDK event. No-op unless debug mode is on. */
    public void log(String message) {
        if (enabled) {
            LOGGER.log(Level.INFO, "[Treblle] {0}", message);
        }
    }

    /** Logs a problem the user should know about. No-op unless debug mode is on. */
    public void warn(String message) {
        if (enabled) {
            LOGGER.log(Level.WARNING, "[Treblle] {0}", message);
        }
    }

    /** Logs an internal SDK failure. No-op unless debug mode is on. */
    public void error(String message, Throwable throwable) {
        if (enabled) {
            LOGGER.log(Level.WARNING, "[Treblle] " + message, throwable);
        }
    }
}
