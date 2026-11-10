package com.flightmonitor.monitor;

import java.nio.file.Path;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Typed runtime settings for the monitor process.
 */
@ConfigurationProperties(prefix = "monitor")
public class MonitorProperties {
    private boolean upstreamPollingEnabled;
    private Path dataDirectory = Path.of("/var/lib/flight-monitor");

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
