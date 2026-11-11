package com.flightmonitor.monitor.lifecycle;

import com.flightmonitor.monitor.MonitorProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.annotation.Order;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Broker-free defaults; Compose explicitly enables lifecycle startup after topic provisioning. */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(LifecycleSettings.class)
public class LifecycleConfiguration {
    @Bean
    Clock lifecycleClock() {
        return Clock.systemUTC();
    }

    @Bean
    CommittedRegistry committedRegistry(LifecycleSettings settings, Clock clock) {
        return new CommittedRegistry(settings, clock);
    }

    @Bean
    @Order(10)
    @Profile("local")
    @ConditionalOnProperty(prefix = "monitor.lifecycle", name = "enabled", havingValue = "true")
    LifecycleRuntime lifecycleRuntime(LifecycleSettings settings, CommittedRegistry gate,
                                      @Value("${spring.kafka.bootstrap-servers}") String servers,
                                      MonitorProperties monitor) {
        return new LifecycleRuntime(settings, gate, servers, monitor);
    }

    @Bean
    @ConditionalOnMissingBean(LifecyclePublisher.class)
    LifecyclePublisher disabledLifecyclePublisher() {
        return new LifecyclePublisher() {
            @Override
            public boolean ready() { return false; }

            @Override
            public CompletableFuture<Void> publish(com.flightmonitor.contracts.event.LocationEventEnvelope event) {
                return CompletableFuture.failedFuture(new IllegalStateException("Lifecycle disabled"));
            }
        };
    }

    @Bean
    LifecycleService lifecycleService(LifecycleSettings settings, CommittedRegistry gate,
                                      LifecyclePublisher publisher, Clock clock) {
        return new LifecycleService(settings, gate, publisher, clock, Duration.ofSeconds(10));
    }

    @Bean
    @Profile("local")
    @ConditionalOnProperty(prefix = "monitor.lifecycle", name = "enabled", havingValue = "true")
    HealthIndicator lifecycleHealthIndicator(LifecyclePublisher publisher) {
        return () -> publisher.ready() ? Health.up().build() : Health.outOfService().build();
    }
}
