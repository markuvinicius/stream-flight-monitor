package com.flightmonitor.poller;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Bootstraps the dedicated upstream poller process.
 */
@SpringBootApplication
@EnableConfigurationProperties(PollerProperties.class)
public class PollerApplication {

    public static void main(String[] args) {
        SpringApplication.run(PollerApplication.class, args);
    }
}
