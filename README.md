# Treblle - Runtime Intelligence Platform

[Website](https://treblle.com/) • [Documentation](https://docs.treblle.com/) • [Pricing](https://treblle.com/pricing)

Discover, Govern, and Secure APIs, Agents, and AI Across Any Cloud, Gateway or Technology.

## Treblle Kafka SDK (Java)

The Treblle Kafka SDK brings Treblle's runtime intelligence to event-driven systems. It wraps your Kafka producer and your consume loop, captures every message together with its headers, payload, timing and outcome, masks sensitive values before anything leaves your JVM, and ships the result to Treblle - asynchronously, on its own threads, with a bounded queue and a circuit breaker so a Treblle outage can never slow down, block or break your application.

Because it decorates the client rather than the wire, it sees the typed value your code passed in - so a JSON message is readable in Treblle without a schema registry.

> **This is a proof of concept.** It is the first Treblle SDK for AsyncAPI-style traffic, and it maps Kafka onto Treblle's existing HTTP-shaped payload so it works against the Treblle dashboard as it exists today. Read [How Kafka maps to Treblle](#how-kafka-maps-to-treblle) and [Known PoC limitations](#known-poc-limitations) before using it for anything that matters.

## Requirements

| | |
|---|---|
| Java | 11 or newer |
| Kafka clients | `org.apache.kafka:kafka-clients` 3.x or 4.x |
| Runtime dependencies | **None.** JSON, GZIP and HTTP all come from the JDK. |

The SDK is compiled with `--release 11`, so the compiler enforces that it only uses Java 11 APIs. This PoC was built and exercised on Java 21; it has not yet been run on an 11 or 17 runtime.

`kafka-clients` is a `provided` dependency - the SDK uses the copy your application already has. It is compiled against 3.9.0.

## Installation

### 1. Install the SDK

The PoC is not on Maven Central yet, so build and install it into your local repository:

```bash
git clone https://github.com/Treblle/treblle-kafka.git
cd treblle-kafka
mvn clean install
```

Then add it to your project.

**Maven**

```xml
<dependency>
    <groupId>com.treblle</groupId>
    <artifactId>treblle-kafka</artifactId>
    <version>1.0.0</version>
</dependency>
```

**Gradle**

```groovy
implementation 'com.treblle:treblle-kafka:1.0.0'
```

### 2. Configure the SDK

Build one `Treblle` instance for the lifetime of your application. The minimum is an SDK Token and an API Key, both from the [Treblle Dashboard](https://app.treblle.com):

```java
import com.treblle.kafka.Treblle;

Treblle treblle = Treblle.builder()
        .sdkToken("YOUR_SDK_TOKEN")
        .apiKey("YOUR_API_KEY")
        .build();
```

**Producing.** Wrap your producer. The second argument is the same `Properties` you built it with - the SDK reads `bootstrap.servers` and `client.id` from it once:

```java
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

Properties props = new Properties();
props.put("bootstrap.servers", "broker-1:9092,broker-2:9092");
props.put("client.id", "checkout-service");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

Producer<String, String> producer = treblle.wrap(new KafkaProducer<>(props), props);

// From here on, nothing changes. Every send is reported to Treblle.
producer.send(new ProducerRecord<>("orders.created", orderId, orderJson));
```

`treblle.wrap(...)` returns a `Producer<K, V>`, so it is a drop-in replacement - transactions, `flush()`, `partitionsFor()` and `metrics()` all delegate to the real producer untouched.

**Consuming.** Kafka has no response, so the SDK asks for the one thing only your code knows: the outcome of processing. Wrap your handler and Treblle gets real processing time and real errors:

```java
import com.treblle.kafka.TreblleConsumerTracker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

Properties consumerProps = new Properties();
consumerProps.put("bootstrap.servers", "broker-1:9092");
consumerProps.put("group.id", "billing-service");
consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
consumer.subscribe(List.of("orders.created"));

TreblleConsumerTracker tracker = treblle.consumerTracker(consumerProps);

while (running) {
    for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(1))) {
        tracker.track(record, r -> orderService.handle(r.value()));
    }
    consumer.commitSync();
}
```

Anything your handler throws propagates unchanged - checked exceptions included, without forcing `throws` onto your consume loop. Treblle observes; it never swallows, wraps or alters your error handling.

If your handler returns a value and you want it to show up as the response body in Treblle, use `trackResult`:

```java
Receipt receipt = tracker.trackResult(record, r -> orderService.handleAndReturn(r.value()));
```

Finally, shut the SDK down with your application so its worker threads stop cleanly:

```java
treblle.close();
```

#### All configuration options

| Option | Type | Required | Default | Description |
|---|---|---|---|---|
| `sdkToken` | `String` | Yes | - | From the Treblle Dashboard. Sent as `sdk_token` in the payload and as the `x-api-key` header on every request to Treblle. |
| `apiKey` | `String` | Yes | - | From the Treblle Dashboard. Sent as `api_key` in the payload. |
| `debug` | `boolean` | No | `false` | Logs all SDK activity locally. See [Debug mode](#debug-mode). |
| `maskedKeywords` | `String...` | No | *(empty)* | Keys whose values are masked before sending. **Empty means masking is skipped entirely.** See [Data masking](#data-masking). |
| `excludedTopics` | `String...` | No | *(empty)* | Topics the SDK must not track. Exact names and `prefix.*` wildcards. See [Excluding topics](#excluding-topics). |
| `ingressEndpoint` | `String` | No | `https://ingress.treblle.com` | Ingestion URL. Point it at a regional endpoint or a self-hosted instance. |
| `enabled` | `boolean` | No | `true` | Master switch. When `false` the SDK sends nothing, even if fully configured. |
| `clusterName` | `String` | No | first bootstrap server host | Friendly name for this Kafka cluster. Folded into `internal_id` (see [Auto API discovery](#auto-api-discovery)) so topics on different clusters never collide into the same auto-discovered API. |

A fully configured instance:

```java
Treblle treblle = Treblle.builder()
        .sdkToken("YOUR_SDK_TOKEN")
        .apiKey("YOUR_API_KEY")
        .debug(true)
        .maskedKeywords("password", "authorization", "ssn", "credit_card")
        .excludedTopics("payments.audit", "internal.*")
        .ingressEndpoint("https://ingress-eu.treblle.com")
        .clusterName("orders-prod-eu")
        .enabled(true)
        .build();
```

#### Environment variables and system properties

Every option can also be set outside your code. Resolution order is **builder value → JVM system property → environment variable → default**, so an explicitly configured value always wins.

| Option | System property | Environment variable |
|---|---|---|
| `sdkToken` | `treblle.sdk.token` | `TREBLLE_SDK_TOKEN` |
| `apiKey` | `treblle.api.key` | `TREBLLE_API_KEY` |
| `debug` | `treblle.debug` | `TREBLLE_DEBUG` |
| `maskedKeywords` | `treblle.masked.keywords` | `TREBLLE_MASKED_KEYWORDS` |
| `excludedTopics` | `treblle.excluded.topics` | `TREBLLE_EXCLUDED_TOPICS` |
| `ingressEndpoint` | `treblle.ingress.endpoint` | `TREBLLE_INGRESS_ENDPOINT` |
| `enabled` | `treblle.enabled` | `TREBLLE_ENABLED` |
| `clusterName` | `treblle.cluster.name` | `TREBLLE_CLUSTER_NAME` |

List options take a comma-separated value; booleans accept `true`, `1` or `yes`. With the environment set, the whole configuration collapses to:

```java
Treblle treblle = Treblle.builder().build();
```

```bash
export TREBLLE_SDK_TOKEN="YOUR_SDK_TOKEN"
export TREBLLE_API_KEY="YOUR_API_KEY"
export TREBLLE_MASKED_KEYWORDS="password,authorization,ssn"
export TREBLLE_EXCLUDED_TOPICS="internal.*"
```

If `sdkToken` or `apiKey` is missing, the SDK **disables itself silently** - it never throws. `treblle.wrap(...)` then hands your producer straight back, unwrapped, so there is not even a wrapper on the path. Turn on debug mode to see why.

### 3. Verify it works

1. Start your application and produce a message to a topic that is not excluded.
2. Open the [Treblle Dashboard](https://app.treblle.com) - the topic auto-discovers as its own API. The message appears as a request against `POST /<topic>`; consumed messages appear as `GET /<topic>`.
3. Nothing showing up? Enable debug mode:

```java
Treblle treblle = Treblle.builder()
        .sdkToken("YOUR_SDK_TOKEN")
        .apiKey("YOUR_API_KEY")
        .debug(true)
        .build();
```

You should see the SDK announce itself at startup and log each send:

```
[Treblle] initialising Treblle Kafka SDK: enabled=true, valid=true, sdkToken=abcd****, ...
[Treblle] Treblle Kafka SDK is active
[Treblle] sending produce payload for topic orders.created (1284 bytes uncompressed)
[Treblle] payload accepted by Treblle (HTTP 200)
```

## Data masking

Masking happens inside your JVM, before anything is sent. Only the keys you list in `maskedKeywords` are masked, matched **case-insensitively**, across request bodies, response bodies and headers.

```java
Treblle treblle = Treblle.builder()
        .sdkToken("YOUR_SDK_TOKEN")
        .apiKey("YOUR_API_KEY")
        .maskedKeywords("password", "ssn", "card_number", "authorization")
        .build();
```

Given this message:

```json
{
  "orderId": "A-1",
  "card_number": "4111111111111111",
  "customer": { "email": "ada@example.com", "password": "hunter123", "SSN": "123-45-6789" },
  "secrets": ["alpha", "bravo"]
}
```

with an `authorization: Bearer eyJhbGciOiJIUzI1` record header, Treblle receives:

```json
{
  "orderId": "A-1",
  "card_number": "****************",
  "customer": { "email": "ada@example.com", "password": "*********", "SSN": "***********" },
  "secrets": ["*****", "*****"]
}
```

```
authorization: Bearer ****************
```

The behaviour in detail:

- Every character of a matching value becomes `*`, so the **length is preserved** and you can still tell a 4-digit PIN from a 32-character token.
- **Keys are always preserved** - only values are masked.
- **Nested objects and arrays** are handled recursively; arrays keep their length and each item is masked individually.
- **Auth schemes survive.** `Bearer abc123` becomes `Bearer ******`, so you can still see which auth type was used.
- **`null` and empty values are skipped** - masking them would only invent data.
- Matching is case-insensitive, so `SSN`, `ssn` and `Ssn` are all covered by listing `ssn` once.

> **An empty `maskedKeywords` list disables masking entirely** and message bodies and headers are sent as-is. The SDK warns about this at startup in debug mode.

## Excluding topics

`excludedTopics` is the Kafka equivalent of excluding paths in Treblle's HTTP SDKs. It accepts exact topic names and trailing wildcards:

```java
Treblle treblle = Treblle.builder()
        .sdkToken("YOUR_SDK_TOKEN")
        .apiKey("YOUR_API_KEY")
        .excludedTopics(
                "payments.audit",   // this topic exactly
                "internal.*")       // and everything starting with "internal."
        .build();
```

Matching is case-sensitive.

The SDK also **always** skips Kafka's own internal topics, regardless of configuration - anything beginning with `__` (`__consumer_offsets`, `__transaction_state`) plus `_schemas` and the Confluent licensing topics. They carry no application traffic and would only add noise. This mirrors how Treblle's HTTP SDKs automatically skip static assets and `.well-known` paths.

In debug mode, every skipped topic is logged.

## Auto API discovery

A single Kafka cluster is usually many independent async APIs sharing one set of brokers - each topic its own channel, often owned and versioned by a different team. The SDK reflects that: **every topic auto-discovers as its own API in the Treblle Dashboard**, not as one API for the whole cluster.

It does this the same way Treblle's HTTP SDKs support multiple logical APIs behind one SDK Token - by sending `internal_id` and `internal_name` in the payload:

| Field | Value |
|---|---|
| `internal_name` | the topic name |
| `internal_id` | `<clusterName>:<topic>` (cluster identity falls back to the first bootstrap server host) |

The cluster identity is folded into `internal_id` - not just the topic name - so that if the same `sdkToken`/`apiKey` pair is ever pointed at two different clusters (e.g. staging and production) that happen to share a topic name, they still auto-discover as two separate APIs rather than merging into one.

You don't need to do anything to enable this: it's automatic as soon as a topic is captured. Set `clusterName` (see [All configuration options](#all-configuration-options)) if you want a friendlier cluster identity than the bootstrap host in that `internal_id`.

> **This changes `internal_id`/`internal_name` on every payload compared to treating the whole cluster as one API.** If you're upgrading from a version of this SDK that sent cluster-level `internal_id`, expect topics to (re-)appear in the Dashboard as new, separate APIs rather than under one existing one.

## Per-message metadata

Attach your own key/value pairs to a message with `TreblleContext`. It is thread-scoped: set it on the thread that calls `send()`, or anywhere inside your consume handler.

```java
import com.treblle.kafka.TreblleContext;

// Producing
TreblleContext.put("tenantId", "acme");
TreblleContext.put("retryCount", 2);
producer.send(new ProducerRecord<>("orders.created", orderId, orderJson));

// Consuming
tracker.track(record, r -> {
    TreblleContext.put("tenantId", tenantOf(r));
    TreblleContext.put("replay", false);
    orderService.handle(r.value());
});
```

The SDK snapshots the context at capture time and clears it immediately, so values never leak into the next message on a pooled thread.

Alongside your values, the SDK adds the Kafka coordinates automatically: `kafka.operation`, `kafka.topic`, `kafka.partition`, `kafka.offset`, `kafka.key`, `kafka.client.id`, `kafka.group.id` and `kafka.timestamp.type`. Treblle caps metadata at 20 properties, keys at 64 characters and values at 128 characters; the SDK's own entries come first and yours fill the rest.

> **Metadata is never masked.** Never put secrets, credentials or personal data there.

## Debug mode

```java
Treblle treblle = Treblle.builder()
        .sdkToken("YOUR_SDK_TOKEN")
        .apiKey("YOUR_API_KEY")
        .debug(true)
        .build();
```

Debug mode is silent by default and logs nothing in production unless you turn it on. When enabled it reports:

- the resolved configuration and whether the SDK actually started (the SDK Token is never logged in full);
- warnings for missing `sdkToken` / `apiKey`, and for an empty `maskedKeywords` list;
- every topic skipped because of `excludedTopics` or because it is a Kafka internal topic;
- every payload sent, Treblle's response, and any HTTP or transport error;
- circuit breaker state changes and dropped payloads;
- any payload field the validator had to repair before sending.

Output goes through `java.util.logging` under the logger name `com.treblle.kafka`, so your existing logging setup - including SLF4J and Log4j JUL bridges - picks it up without the SDK taking a logging dependency.

**Troubleshooting checklist**

| Symptom | Likely cause |
|---|---|
| `SDK disabled` at startup | `sdkToken` or `apiKey` missing, or `enabled=false` |
| `topic '...' is excluded; skipping` | `excludedTopics` matched, or it is a Kafka internal topic |
| `circuit breaker open; dropping payloads` | Treblle returned 4xx/5xx - usually a bad SDK Token |
| Nothing logged at all | Debug mode is off, or your logging config filters `com.treblle.kafka` |

## How Kafka maps to Treblle

Treblle's payload schema was built for synchronous HTTP APIs. This SDK maps Kafka onto it **without changing a single schema field**, so the PoC works against the Treblle dashboard exactly as it exists today.

| Treblle field | Producing | Consuming |
|---|---|---|
| `request.method` | `POST` | `GET` |
| `request.route_path` | topic name | topic name |
| `request.url` | `kafka://broker:9092/<topic>` | same |
| `request.headers` | record headers | record headers |
| `request.body` | the record value | the record value |
| `request.timestamp` | record timestamp, UTC | record timestamp, UTC |
| `response.code` | `200`, or a mapped code on failure | `200`, or a mapped code if the handler throws |
| `response.load_time` | `send()` to broker acknowledgement | your handler's wall time |
| `response.size` | serialised key + value bytes | serialised key + value bytes |
| `response.body` | the broker's ack receipt (`topic`, `partition`, `offset`) | your handler's return value |
| `response.headers` | `kafka-topic`, `kafka-partition`, `kafka-offset`, `kafka-timestamp` | plus `kafka-group-id`, `kafka-lag-ms` |
| `errors[]` | send failure, `source: onError` | handler throwable, `source: onException` |
| `queries[]` | always empty - Kafka has no SQL layer | same |
| `internal_name` | topic name | same |
| `internal_id` | `<clusterName>:<topic>` | same |

**Why `POST` and `GET`?** The schema's `method` field is an enum of HTTP verbs. Rather than break it, producing maps to `POST` and consuming maps to `GET`. In practice this reads well: producing to `orders.created` shows up as `POST /orders.created` and consuming it as `GET /orders.created` - the producer view and the consumer view of the same channel, as two distinct endpoints. The true semantic is preserved in `metadata["kafka.operation"]` (`produce` / `consume`), so Treblle can switch to real AsyncAPI verbs later without a breaking payload change.

**Kafka failures become status codes.** A send that times out reads as `504`, one rejected by ACLs as `403`, a record over `max.request.size` as `413`, an unknown topic as `404`, and anything Kafka considers retriable as `503`. The original exception class, message, file and line are always in `errors[]`.

### Why it never slows your application down

- The wrapper does almost nothing on your thread: it grabs cheap references to the topic, key, value, headers and metadata, and returns. Masking, JSON, GZIP and HTTP all happen later.
- The producer's acknowledgement callback runs on Kafka's I/O thread, so it only pushes onto a bounded queue and returns.
- One low-priority daemon thread drains that queue and builds payloads. Two more handle the HTTP.
- The queue holds 1,000 captures and **drops the oldest** when full, so a burst or a stalled network can never grow your memory.
- Sends are fire-and-forget with a 2-second connect and 3-second request timeout, and no retries.
- A lock-free circuit breaker watches Treblle's responses. On 4xx/5xx it opens and **drops** payloads (never queues or delays them) with exponential backoff from 1s to 60s, ±20% jitter. A `429` with a `Retry-After` header is honoured, capped at 60 seconds. When the window expires it lets exactly one probe through; two consecutive successes close it again.
- Every SDK operation is wrapped in try/catch. An internal SDK failure is logged in debug mode and otherwise ignored.

In the verification harness, 200 sends against a completely unreachable Ingress endpoint cost 6 ms in total and threw nothing.

## Known PoC limitations

Be aware of these before relying on the SDK:

- **No test suite or CI.** This PoC ships with the SDK and this README only. It was verified with a manual harness covering payload shape, masking, body edge cases, topic exclusion, config precedence, failure mapping, transport safety and the circuit breaker (157 checks), but that harness is not part of the repository.
- **No Avro, Protobuf or Schema Registry decoding.** The SDK reads the typed value your code passed to the producer. JSON strings, `byte[]` containing JSON, `Map`/`List` structures and scalars are captured faithfully. A binary Avro or Protobuf payload is replaced with `{"message": "Message payload is not valid JSON.", "type": "application/octet-stream", "size": ...}` - you still see that a message flowed, just not its contents.
- **The record value is captured by reference, not deep-copied.** Deep-copying every message would defeat the zero-overhead design. If your application mutates a value object after `send()` returns, Treblle may capture the mutated state. Sending immutable values or strings - which almost all producers do - avoids this entirely.
- **Consuming requires wrapping your handler.** There is no zero-code-change option on the consume side, because processing time and handler errors are things only your code knows. A `ConsumerInterceptor` could capture messages at `poll()` with no code change, but it would report no processing time and no errors.
- **Kafka Streams and Spring Kafka are not covered.** Only the plain `Producer` and `ConsumerRecord` APIs.
- **`response.size` depends on the broker's acknowledgement.** Some clients (and `MockProducer`) report `-1` for serialised sizes, in which case the size is reported as `0`.
- **Compiled against `kafka-clients` 3.9.0.** Older 3.x clients work; calling a 3.7+ method such as `clientInstanceId(Duration)` through the wrapper on an older client would fail, as it would without the wrapper.

## Support

Full documentation lives at [docs.treblle.com](https://docs.treblle.com/).

Found a bug or have a question about this SDK? Open an issue on [GitHub Issues](https://github.com/Treblle/treblle-kafka/issues).

## License

The MIT License (MIT). See [LICENSE](LICENSE) for details.
