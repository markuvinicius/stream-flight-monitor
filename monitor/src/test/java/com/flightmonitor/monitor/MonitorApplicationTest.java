package com.flightmonitor.monitor;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "monitor.lifecycle.enabled=false")
class MonitorApplicationTest {
    @Autowired
    private MonitorProperties monitorProperties;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ApplicationContext applicationContext;

    @LocalServerPort
    private int port;

    @Test
    void startsWithUpstreamPollingDisabled() {
        assertThat(monitorProperties.isUpstreamPollingEnabled()).isFalse();
    }

    @Test
    void exposesActuatorHealth() {
        ResponseEntity<String> response =
                restTemplate.getForEntity("http://localhost:" + port + "/actuator/health", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("\"status\":\"UP\"");
    }

    @Test
    void exposes_readiness_and_does_not_create_kafka_admin_clients_outside_local_profile() {
        ResponseEntity<String> response =
                restTemplate.getForEntity("http://localhost:" + port + "/actuator/health/readiness", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("\"status\":\"UP\"");
        assertThat(applicationContext.containsBean("kafkaTopicProvisioner")).isFalse();
        assertThat(applicationContext.getBeansOfType(
                com.flightmonitor.monitor.lifecycle.LifecycleRuntime.class)).isEmpty();
        assertThat(applicationContext.getBean(com.fasterxml.jackson.databind.ObjectMapper.class)
                .getFactory().isEnabled(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION)).isTrue();
    }
}
