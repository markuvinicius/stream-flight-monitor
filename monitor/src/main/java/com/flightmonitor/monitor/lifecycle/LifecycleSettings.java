package com.flightmonitor.monitor.lifecycle;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Validated limits for the single-monitor local lifecycle deployment.
 * Changing namespace, application ID or store names requires a recovery migration.
 */
@ConfigurationProperties("monitor.lifecycle")
public record LifecycleSettings(
        boolean enabled, String namespace, String applicationId, String publicOrigin,
        int maxActive, int maxLifetime, int maxOperationsPerLocation) {
    public LifecycleSettings {
        namespace = namespace == null ? "flight-local-v1" : namespace;
        applicationId = applicationId == null ? "flight-monitor-lifecycle-v1" : applicationId;
        publicOrigin = publicOrigin == null ? "http://127.0.0.1:8080" : publicOrigin;
        maxActive = maxActive == 0 ? 1_000 : maxActive;
        maxLifetime = maxLifetime == 0 ? 10_000 : maxLifetime;
        maxOperationsPerLocation = maxOperationsPerLocation == 0 ? 16 : maxOperationsPerLocation;
        if (!namespace.matches("[A-Za-z0-9._-]{1,80}")
                || !applicationId.matches("[A-Za-z0-9._-]{1,100}")
                || !publicOrigin.matches("http://(127\\.0\\.0\\.1|localhost)(:[0-9]{1,5})?")
                || maxActive < 1 || maxLifetime < maxActive || maxLifetime > 100_000
                || maxOperationsPerLocation < 2 || maxOperationsPerLocation > 64) {
            throw new IllegalArgumentException("Invalid local lifecycle configuration");
        }
    }
}
