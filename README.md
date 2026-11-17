# Treblle - Runtime Intelligence Platform

[Website](https://treblle.com/) • [Documentation](https://docs.treblle.com/) • [Pricing](https://treblle.com/pricing)

Discover, Govern, and Secure APIs, Agents, and AI Across Any Cloud, Gateway or Technology.

## Treblle Kafka SDK (Java)

The Treblle Kafka SDK brings Treblle's runtime intelligence to event-driven systems. It wraps your Kafka producer and your consume loop and captures each message with its headers, payload, timing and outcome. Sensitive values are masked inside your JVM before anything leaves it, and the result is shipped to Treblle asynchronously on the SDK's own threads. Bounded memory, a cap on in-flight requests and a circuit breaker keep a Treblle outage from slowing down or breaking your application.

Because it decorates the client instead of sitting on the wire, it sees the typed value your code passed in. JSON strings, JSON bytes and `Map`/`List` values are readable in Treblle without a schema registry, and POJOs are too once you plug in your JSON serialiser (see [Message bodies](#message-bodies)).

## Requirements

| | |
|---|---|
| Java | 11 or newer (CI-tested on 11, 17 and 21) |
| Kafka clients | `org.apache.kafka:kafka-clients` 3.0 to 4.x (CI-tested on 3.0.0, 3.9.0 and 4.1.0) |
| Runtime dependencies | None. JSON, GZIP and HTTP all come from the JDK. |

`kafka-clients` is a `provided` dependency: the SDK uses the copy your application already has. The published jar is compiled against 3.9.0, and CI runs that same build against 3.0.0 and 4.1.0.

## Installation

### 1. Add the dependency

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

Build one `Treblle` instance for the lifetime of your application. At minimum it needs an SDK Token and an API Key, both from the [Treblle Dashboard](https://app.treblle.com):

```java
import com.treblle.kafka.Treblle;

Treblle treblle = Treblle.builder()
        .sdkToken("YOUR_SDK_TOKEN")
        .apiKey("YOUR_API_KEY")
        .build();
```

**Producing.** Wrap your producer. The second argument is the same `Properties` you built it with; the SDK reads `bootstrap.servers` and `client.id` from it once:

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

`treblle.wrap(...)` returns a `Producer<K, V>`, so declare your variable as `Producer` rather than `KafkaProducer`. Transactions, `flush()`, `partitionsFor()`, `metrics()` and the version-specific methods (`clientInstanceId`, `registerMetricForSubscription`, ...) all delegate to the real producer unchanged.

**Consuming.** Kafka has no response, so the SDK needs the one thing only your code knows: the outcome of processing. Wrap your handler and Treblle gets real processing time and real errors:

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

Anything your handler throws propagates unchanged, checked exceptions included, and you don't have to add `throws` to your consume loop. The SDK only observes and leaves your error handling alone.

If your handler returns a value and you want it to show up as the response body in Treblle, use `trackResult`:

```java
Receipt receipt = tracker.trackResult(record, r -> orderService.handleAndReturn(r.value()));
```

**Shutting down.** Close the SDK with your application. `close()` sends anything still queued (waiting up to 5 seconds), then stops the SDK's threads:

```java
treblle.close();                         // or treblle.close(Duration.ofSeconds(10))
```

If you forget, a JVM shutdown hook does the same on a normal exit. For batch jobs and tests, `treblle.flush(Duration)` waits until everything captured so far has been sent, without shutting anything down.

#### Spring Kafka

Spring Kafka isn't integrated automatically, but both sides take a few lines. On the producing side, register a post-processor on your producer factory:

```java
@Bean
public ProducerFactory<String, Order> producerFactory(Treblle treblle) {
    DefaultKafkaProducerFactory<String, Order> factory = new DefaultKafkaProducerFactory<>(props());
    factory.addPostProcessor(producer -> treblle.wrap(producer, factory.getConfigurationProperties()));
    return factory;
}
```

On the consuming side, take the `ConsumerRecord` in your listener and wrap the body:

```java
@KafkaListener(topics = "orders.created")
public void onOrder(ConsumerRecord<String, Order> record) {
    tracker.track(record, r -> orderService.handle(r.value()));
}
```

#### All configuration options

| Option | Type | Required | Default | Description |
|---|---|---|---|---|
| `sdkToken` | `String` | Yes | - | From the Treblle Dashboard. Sent as `sdk_token` in the payload and as the `x-api-key` header on every request to Treblle. |
| `apiKey` | `String` | Yes | - | From the Treblle Dashboard. Sent as `api_key` in the payload. |
| `debug` | `boolean` | No | `false` | Logs all SDK activity locally. See [Debug mode](#debug-mode). |
| `maskedKeywords` | `String...` | No | *(none)* | Extra keys to mask, **on top of** the built-in defaults. See [Data masking](#data-masking). |
| `includedTopics` | `String...` | No | *(empty)* | The only topics the SDK tracks. Empty means every topic is a candidate. Exact names and `prefix.*` wildcards. See [Including and excluding topics](#including-and-excluding-topics). |
| `excludedTopics` | `String...` | No | *(empty)* | Topics the SDK must not track. Exact names and `prefix.*` wildcards. |
| `captureRecordKeys` | `boolean` | No | `true` | Whether record keys are reported as `kafka.key` metadata. Turn off if your keys contain personal data such as email addresses. |
| `valueRenderer` | `ValueRenderer` | No | *(none)* | Turns POJO values into JSON, e.g. `objectMapper::writeValueAsString`. See [Message bodies](#message-bodies). |
| `ingressEndpoint` | `String` | No | `https://ingress.treblle.com` | Ingestion URL. Must be `https`; plain `http` is accepted only for `localhost`. |
| `enabled` | `boolean` | No | `true` | Master switch. When `false` the SDK sends nothing, even if fully configured. |
| `clusterName` | `String` | No | first bootstrap server host | Friendly name for this Kafka cluster. Included in `internal_id` (see [Auto API discovery](#auto-api-discovery)) so topics on different clusters never merge into the same auto-discovered API. |

A fully configured instance:

```java
Treblle treblle = Treblle.builder()
        .sdkToken("YOUR_SDK_TOKEN")
        .apiKey("YOUR_API_KEY")
        .debug(true)
        .maskedKeywords("email", "iban")
        .includedTopics("orders.*", "payments.*")
        .excludedTopics("payments.audit", "internal.*")
        .captureRecordKeys(false)
        .valueRenderer(objectMapper::writeValueAsString)
        .ingressEndpoint("https://ingress-eu.treblle.com")
        .clusterName("orders-prod-eu")
        .enabled(true)
        .build();
```

#### Environment variables and system properties

Every option except `valueRenderer` can also be set outside your code. Values resolve in this order: builder value, JVM system property, environment variable, default. A value set on the builder always wins, even an empty list: `.excludedTopics()` with no arguments means "exclude nothing", whatever the environment says.

| Option | System property | Environment variable |
|---|---|---|
| `sdkToken` | `treblle.sdk.token` | `TREBLLE_SDK_TOKEN` |
| `apiKey` | `treblle.api.key` | `TREBLLE_API_KEY` |
| `debug` | `treblle.debug` | `TREBLLE_DEBUG` |
| `maskedKeywords` | `treblle.masked.keywords` | `TREBLLE_MASKED_KEYWORDS` |
| `includedTopics` | `treblle.included.topics` | `TREBLLE_INCLUDED_TOPICS` |
| `excludedTopics` | `treblle.excluded.topics` | `TREBLLE_EXCLUDED_TOPICS` |
| `captureRecordKeys` | `treblle.capture.record.keys` | `TREBLLE_CAPTURE_RECORD_KEYS` |
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
export TREBLLE_MASKED_KEYWORDS="email,iban"
export TREBLLE_EXCLUDED_TOPICS="internal.*"
```

If `sdkToken` or `apiKey` is missing, the SDK **disables itself** and never throws. `treblle.wrap(...)` then returns your producer unwrapped, so no wrapper sits on the path at all. Turn on debug mode to see why.

### 3. Verify it works

1. Start your application and produce a message to a topic that is not excluded.
2. Open the [Treblle Dashboard](https://app.treblle.com). The topic auto-discovers as its own API. Produced messages appear as requests against `POST /<topic>`; consumed messages appear as `GET /<topic>`.
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

Masking happens inside your JVM before anything is sent, and it is **always on**. These keys are masked by default, matched case-insensitively:

`password`, `pwd`, `secret`, `password_confirmation`, `passwordConfirmation`, `cc`, `card_number`, `cardNumber`, `ccv`, `cvv`, `ssn`, `credit_score`, `creditScore`, `authorization`, `proxy-authorization`, `api_key`, `apiKey`, `x-api-key`, `access_token`, `refresh_token`, `client_secret`, `token`, `cookie`, `set-cookie`

`maskedKeywords` adds to this list. It never replaces it.

```java
Treblle treblle = Treblle.builder()
        .sdkToken("YOUR_SDK_TOKEN")
        .apiKey("YOUR_API_KEY")
        .maskedKeywords("email", "secrets")
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
  "customer": { "email": "***************", "password": "*********", "SSN": "***********" },
  "secrets": ["*****", "*****"]
}
```

```
authorization: Bearer ****************
```

The rules in detail:

- Masking applies to request bodies, response bodies, record headers and your [per-message metadata](#per-message-metadata) (by metadata key).
- Error messages and record keys can't be masked by key, so the SDK scrubs `keyword=value` and `keyword: value` patterns in them. `login failed for password=hunter2` becomes `login failed for password=*******`.
- Every character of a matching value becomes `*`. Length is preserved, so you can still tell a 4-digit PIN from a 32-character token.
- Keys are never masked, only values.
- Nested objects and arrays are masked recursively. Arrays keep their length, and each item is masked individually.
- Auth schemes are kept: `Bearer abc123` becomes `Bearer ******`, so you can still see which auth type was used.
- `null` and empty values are left alone, since masking them would invent data.

What masking can't protect: topic names, header *names*, and free-form text that doesn't follow a `keyword=value` shape. If your record keys are personal data (an email address, say), set `captureRecordKeys(false)`.

## Message bodies

The SDK reads the value your code handed to the producer (or the deserialised value on the consumer side) and turns it into a JSON body:

| Value type | What Treblle shows |
|---|---|
| `String` / `CharSequence` containing JSON | the parsed JSON |
| `byte[]` / `ByteBuffer` containing UTF-8 JSON | the parsed JSON |
| `Map`, `List` and other `Collection`s | the structure as JSON |
| `Number`, `Boolean` | the scalar |
| `null` (a tombstone) | `null` |
| Binary data (Avro, Protobuf, ...) | `{"message": "Message payload is not valid JSON.", "type": "application/octet-stream", "size": ...}` |
| Anything else (POJOs, Avro `GenericRecord`) | your `valueRenderer`'s output, or else the value's `toString()` if that is valid JSON |

POJOs are the common case with JSON serialisers, so plug in the one you already use:

```java
ObjectMapper mapper = new ObjectMapper();

Treblle treblle = Treblle.builder()
        .sdkToken("YOUR_SDK_TOKEN")
        .apiKey("YOUR_API_KEY")
        .valueRenderer(mapper::writeValueAsString)
        .build();
```

The renderer runs on the SDK's worker thread, never yours, and anything it throws is contained. Bodies over 2 MB are replaced with a short summary of their size. Oversized strings and byte arrays are detected and released on your thread, so the SDK never holds on to them.

## Including and excluding topics

`includedTopics` and `excludedTopics` work like the include and exclude path options in Treblle's HTTP SDKs. Both accept exact topic names and trailing wildcards (`orders.*` matches every topic starting with `orders.`), and matching is case-sensitive.

Both are empty by default, so every topic is tracked. Setting `includedTopics` turns it into an allow-list: only matching topics are tracked, and everything else is treated as excluded.

```java
Treblle treblle = Treblle.builder()
        .sdkToken("YOUR_SDK_TOKEN")
        .apiKey("YOUR_API_KEY")
        .includedTopics(
                "orders.*",         // only topics starting with "orders."
                "payments.*")       // and "payments."
        .excludedTopics(
                "payments.audit",   // except this topic exactly
                "internal.*")       // and anything starting with "internal."
        .build();
```

You can use both at once. `excludedTopics` is applied on top of `includedTopics` and wins when a topic matches both. The SDK decides each topic like this:

1. Kafka-internal topics are always excluded (see below). This check runs first, whatever the configuration.
2. If `includedTopics` is non-empty, the topic must match it or it's excluded.
3. If the topic matches `excludedTopics`, it's excluded, even if it matched `includedTopics`.

With an empty `includedTopics` (the default), step 2 is skipped and `excludedTopics` alone decides what's left out.

The SDK also always skips Kafka's own internal topics: anything beginning with `__` (`__consumer_offsets`, `__transaction_state`), plus `_schemas` and the Confluent licensing topics. They carry no application traffic. Treblle's HTTP SDKs skip static assets and `.well-known` paths for the same reason.

In debug mode, every skipped topic is logged.

## Auto API discovery

A single Kafka cluster usually hosts many independent async APIs on one set of brokers. Each topic is its own channel, often owned and versioned by a different team, so every topic auto-discovers as its own API in the Treblle Dashboard instead of the whole cluster showing up as one.

This uses the mechanism Treblle's HTTP SDKs already use to support several logical APIs behind one SDK Token. The SDK sends `internal_id` and `internal_name` in the payload:

| Field | Value |
|---|---|
| `internal_name` | the topic name |
| `internal_id` | `<clusterName>:<topic>` (cluster identity falls back to the first bootstrap server host) |

`internal_id` includes the cluster identity as well as the topic name. If the same `sdkToken`/`apiKey` pair is pointed at two clusters (staging and production, say) that share a topic name, the two topics still show up as separate APIs.

Discovery is automatic once a topic is captured. Set `clusterName` if you want a friendlier cluster identity in `internal_id` than the bootstrap host.

## Per-message metadata

Attach your own key/value pairs to a message with `TreblleContext`. It is thread-scoped, so set it on the thread that calls `send()`, or anywhere inside your consume handler.

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

How long metadata lives:

- **Producing:** every `send()` through a wrapped producer takes the metadata and clears it, even when the topic is excluded, so values never leak into the next message on a pooled thread.
- **Consuming:** metadata set inside the handler belongs to that record and is cleared when the handler returns, whether or not the topic is tracked.
- **Consume-then-produce:** a `send()` made from inside a handler is tagged with a copy of the handler's metadata, so both the consumed and the produced message carry it.

The SDK also adds the Kafka coordinates automatically: `kafka.operation`, `kafka.topic`, `kafka.partition`, `kafka.offset`, `kafka.key`, `kafka.client.id`, `kafka.group.id` and `kafka.timestamp.type`. Treblle caps metadata at 20 properties, keys at 64 characters and values at 128 characters. The SDK's own entries come first, and yours fill the remaining slots.

Metadata whose key is a masked keyword is masked. Even so, don't put secrets or personal data here.

## Debug mode

```java
Treblle treblle = Treblle.builder()
        .sdkToken("YOUR_SDK_TOKEN")
        .apiKey("YOUR_API_KEY")
        .debug(true)
        .build();
```

Debug mode is off by default. When enabled it reports:

- the resolved configuration and whether the SDK actually started (the SDK Token is never logged in full);
- warnings for a missing `sdkToken` / `apiKey`;
- every topic skipped because of `includedTopics`/`excludedTopics` or because it is a Kafka internal topic;
- every payload sent, Treblle's response, and any HTTP or transport error;
- circuit breaker state changes and dropped payloads;
- any payload field the validator had to repair before sending.

Two problems are logged even with debug mode off, once each, because otherwise the SDK would silently send nothing: Treblle rejecting your credentials (HTTP 401/403), and an `ingressEndpoint` that isn't a valid `https` URL.

Output goes through `java.util.logging` under the logger name `com.treblle.kafka`. Your existing logging setup picks it up, including SLF4J and Log4j JUL bridges, and the SDK needs no logging dependency of its own.

**Troubleshooting checklist**

| Symptom | Likely cause |
|---|---|
| `SDK disabled` at startup | `sdkToken` or `apiKey` missing, or `enabled=false` |
| `Treblle rejected the SDK credentials` | Wrong `sdkToken` or `apiKey` |
| `ingressEndpoint ... is not an https URL` | The endpoint uses `http` for a non-local host, or is malformed |
| `topic '...' is excluded; skipping` | `excludedTopics` matched, `includedTopics` didn't, or it is a Kafka internal topic |
| `circuit breaker open; dropping payloads` | Treblle returned 5xx, 429, 401 or 403, or was unreachable |
| `Treblle rejected a payload (HTTP 4xx)` | That one payload was refused (e.g. too large); other payloads keep flowing |
| Nothing logged at all | Debug mode is off, or your logging config filters `com.treblle.kafka` |

### Why it never slows your application down

- On your thread, the wrapper only grabs references to the topic, key, value, headers and metadata, plus O(1) size checks, then returns. Copying, masking, JSON, GZIP and HTTP all happen later.
- The producer's acknowledgement callback runs on Kafka's I/O thread, so it only pushes onto a bounded queue and returns.
- One low-priority daemon thread drains that queue and builds payloads, and two more handle HTTP.
- Memory has a hard ceiling. The queue holds at most 1,000 captures (dropping the oldest when full) and at most about 32 MB of referenced message values (dropping new captures when over budget). At most 8 requests to Treblle are in flight at once; when Treblle is slow, the queue absorbs the backlog and drops rather than piling up requests.
- Sends have a 2-second connect timeout, a 3-second request timeout, and no retries.
- A lock-free circuit breaker watches Treblle's responses. On a 5xx, a 429, a 401/403 or a network failure it opens and drops payloads instead of queuing or delaying them, backing off exponentially from 1s to 60s with ±20% jitter. A `429` with a `Retry-After` header is honoured, capped at 60 seconds. When the window expires it lets one probe through at a time, and two consecutive successes close it again. Any other 4xx (a `400` or `413` for one bad payload) drops just that payload and doesn't open the breaker.
- Every SDK operation is wrapped in try/catch. Internal SDK failures are logged in debug mode and otherwise ignored.

## Support

Full documentation lives at [docs.treblle.com](https://docs.treblle.com/).

Found a bug or have a question about this SDK? Open an issue on [GitHub Issues](https://github.com/Treblle/treblle-kafka/issues).

## License

The MIT License (MIT). See [LICENSE](LICENSE) for details.
