package com.flightmonitor.poller;

import java.net.HttpURLConnection;
import java.net.URI;

/**
 * Small dependency-free Docker health check for the poller actuator endpoint.
 */
public final class HealthProbe {
    private HealthProbe() {
    }

    public static void main(String[] args) throws Exception {
        HttpURLConnection connection =
                (HttpURLConnection) URI.create("http://127.0.0.1:8080/actuator/health")
                        .toURL()
                        .openConnection();
        connection.setConnectTimeout(1_500);
        connection.setReadTimeout(1_500);
        try {
            if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
                System.exit(1);
            }
        } finally {
            connection.disconnect();
        }
    }
}
