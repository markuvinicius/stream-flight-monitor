package com.flightmonitor.contracts;

public final class KafkaTopics {
    public static final String LOCATION_EVENTS = "flight.location-events.v1";
    public static final String LOCATION_REGISTRY = "flight.location-registry.v1";
    public static final String POLLER_CHECKPOINT = "flight.poller-checkpoint.v1";

    private KafkaTopics() {
    }
}
