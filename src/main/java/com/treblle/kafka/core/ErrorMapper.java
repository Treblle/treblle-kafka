package com.treblle.kafka.core;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Translates a Kafka or application failure into Treblle's HTTP-shaped error model.
 *
 * <p>Treblle's payload schema is built for HTTP, so a failure has to become a status code plus an
 * entry in the {@code errors} array. The mapping below picks the status code that best describes
 * what went wrong, so a send that timed out reads as a 504 and one rejected by ACLs reads as a
 * 403.
 *
 * <p>This class deliberately matches on exception <em>class names</em> rather than importing the
 * Kafka exception hierarchy, which keeps the whole {@code core} package free of Kafka types and
 * unit-testable in isolation.
 */
public final class ErrorMapper {

    /** Error occurred while the broker was acknowledging a produced record. */
    public static final String SOURCE_ON_ERROR = "onError";

    /** Error was thrown by the application's consume handler. */
    public static final String SOURCE_ON_EXCEPTION = "onException";

    private static final Map<String, Integer> STATUS_BY_EXCEPTION = new HashMap<>();

    static {
        STATUS_BY_EXCEPTION.put("TimeoutException", 504);
        STATUS_BY_EXCEPTION.put("RecordTooLargeException", 413);
        STATUS_BY_EXCEPTION.put("MessageSizeTooLargeException", 413);
        STATUS_BY_EXCEPTION.put("AuthorizationException", 403);
        STATUS_BY_EXCEPTION.put("TopicAuthorizationException", 403);
        STATUS_BY_EXCEPTION.put("GroupAuthorizationException", 403);
        STATUS_BY_EXCEPTION.put("ClusterAuthorizationException", 403);
        STATUS_BY_EXCEPTION.put("TransactionalIdAuthorizationException", 403);
        STATUS_BY_EXCEPTION.put("SaslAuthenticationException", 401);
        STATUS_BY_EXCEPTION.put("AuthenticationException", 401);
        STATUS_BY_EXCEPTION.put("UnknownTopicOrPartitionException", 404);
        STATUS_BY_EXCEPTION.put("InvalidTopicException", 400);
        STATUS_BY_EXCEPTION.put("SerializationException", 400);
        STATUS_BY_EXCEPTION.put("InvalidRecordException", 400);
        STATUS_BY_EXCEPTION.put("NotEnoughReplicasException", 503);
        STATUS_BY_EXCEPTION.put("NotEnoughReplicasAfterAppendException", 503);
        STATUS_BY_EXCEPTION.put("NotLeaderOrFollowerException", 503);
        STATUS_BY_EXCEPTION.put("NetworkException", 503);
        STATUS_BY_EXCEPTION.put("DisconnectException", 503);
        STATUS_BY_EXCEPTION.put("CoordinatorNotAvailableException", 503);
        STATUS_BY_EXCEPTION.put("BrokerNotAvailableException", 503);
    }

    private ErrorMapper() {
    }

    /**
     * Unwraps the wrapper exceptions Kafka's async APIs use so the mapping sees the real cause.
     */
    public static Throwable unwrap(Throwable throwable) {
        Throwable current = throwable;
        int guard = 0;
        while (current != null && guard++ < 10) {
            boolean isWrapper = current instanceof java.util.concurrent.ExecutionException
                    || current instanceof java.util.concurrent.CompletionException;
            if (!isWrapper || current.getCause() == null) {
                return current;
            }
            current = current.getCause();
        }
        return throwable;
    }

    /** The HTTP status code that best describes {@code throwable}. */
    public static int statusFor(Throwable throwable) {
        if (throwable == null) {
            return 200;
        }
        Throwable root = unwrap(throwable);
        Integer mapped = STATUS_BY_EXCEPTION.get(root.getClass().getSimpleName());
        if (mapped != null) {
            return mapped;
        }
        // Anything Kafka considers retriable is a transient broker-side condition.
        if (isSubclassOf(root.getClass(), "RetriableException")) {
            return 503;
        }
        return 500;
    }

    /** Builds one entry for the payload's {@code errors} array. */
    public static Map<String, Object> toError(Throwable throwable, String source) {
        Throwable root = unwrap(throwable);
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("source", source);
        error.put("type", root.getClass().getSimpleName());
        error.put("message", root.getMessage() == null
                ? root.getClass().getName()
                : root.getMessage());

        String file = "";
        int line = 0;
        StackTraceElement[] stack = root.getStackTrace();
        if (stack != null && stack.length > 0) {
            StackTraceElement origin = firstApplicationFrame(stack);
            if (origin.getFileName() != null) {
                file = origin.getFileName();
            } else {
                file = origin.getClassName();
            }
            if (origin.getLineNumber() > 0) {
                line = origin.getLineNumber();
            }
        }
        error.put("file", file);
        error.put("line", line);
        return error;
    }

    /**
     * Skips SDK frames so the reported file and line point at the application or Kafka code that
     * actually failed, not at Treblle's wrapper.
     */
    private static StackTraceElement firstApplicationFrame(StackTraceElement[] stack) {
        for (StackTraceElement element : stack) {
            if (!element.getClassName().startsWith("com.treblle.kafka")) {
                return element;
            }
        }
        return stack[0];
    }

    private static boolean isSubclassOf(Class<?> type, String simpleName) {
        Class<?> current = type;
        while (current != null) {
            if (simpleName.equals(current.getSimpleName())) {
                return true;
            }
            current = current.getSuperclass();
        }
        return false;
    }
}
