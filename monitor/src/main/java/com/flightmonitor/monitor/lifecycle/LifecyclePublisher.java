package com.flightmonitor.monitor.lifecycle;

import com.flightmonitor.contracts.event.LocationEventEnvelope;

import java.util.concurrent.CompletableFuture;

/** Publishes a single location-keyed command within an explicit Kafka transaction. */
public interface LifecyclePublisher {
    /** Whether Kafka processing and the committed projection can safely accept mutations. */
    boolean ready();

    /** Schedules one bounded asynchronous transaction; completion alone is not a serving acknowledgement. */
    CompletableFuture<Void> publish(LocationEventEnvelope event);
}
