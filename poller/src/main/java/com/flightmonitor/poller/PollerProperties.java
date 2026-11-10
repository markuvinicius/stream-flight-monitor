package com.flightmonitor.poller;

import java.nio.file.Path;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Typed runtime settings for the poller process.
 */
@ConfigurationProperties(prefix = "poller")
public class PollerProperties {
    private boolean upstreamPollingEnabled;
    private Path dataDirectory = Path.of("/var/lib/flight-poller");

    public boolean isUpstreamPollingEnabled() {
        return upstreamPollingEnabled;
    }

    public void setUpstreamPollingEnabled(boolean upstreamPollingEnabled) {
        this.upstreamPollingEnabled = upstreamPollingEnabled;
    }

    public Path getDataDirectory() {
        return dataDirectory;
    }

    public void setDataDirectory(Path dataDirectory) {
        this.dataDirectory = dataDirectory;
    }
}
