package com.flightmonitor.monitor.api;

import com.flightmonitor.monitor.lifecycle.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Instant;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class LifecycleApiTest {
    private LifecycleService service;
    private MockMvc mvc;
    private static final String ID = "a".repeat(64);

    @BeforeEach
    void start() {
        service = mock(LifecycleService.class);
        mvc = MockMvcBuilders.standaloneSetup(new LifecycleController(service))
                .setControllerAdvice(new LifecycleErrors())
                .addFilters(new LocalApiProtection(new LifecycleSettings(false, null, null, null, 0, 0, 0)))
                .build();
    }

    private MockHttpServletRequestBuilder registration(String json) {
        return post("/api/locations").header("Host", "127.0.0.1:8080")
                .header("X-Flight-Monitor", "1").header("Idempotency-Key", "key")
                .contentType(MediaType.APPLICATION_JSON).content(json);
    }

    @Test
    void registrationReturns201ReceiptAndNoStoreFallback() throws Exception {
        when(service.register("key", 90, -180)).thenReturn(new LifecycleService.Registration(
                ID, ID, "/locations/" + ID, Instant.parse("2026-11-11T12:00:00Z"), "WAITING_FOR_FIRST_POLL"));
        mvc.perform(registration("{\"latitude\":90,\"longitude\":-180}"))
                .andExpect(status().isCreated()).andExpect(header().string("Cache-Control", "no-store"))
                .andExpect(jsonPath("$.locationId").value(ID))
                .andExpect(jsonPath("$.state").value("WAITING_FOR_FIRST_POLL"));
    }

    @Test
    void rejectsMalformedMissingStringNullAndUnknownFields() throws Exception {
        for (String json : new String[]{"{", "{}", "null", "{\"latitude\":0}", "{\"latitude\":null,\"longitude\":0}",
                "{\"latitude\":\"0\",\"longitude\":0}", "{\"latitude\":0,\"longitude\":0,\"extra\":1}"}) {
            mvc.perform(registration(json)).andExpect(status().isBadRequest())
                    .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
        }
        verifyNoInteractions(service);
    }

    @Test
    void limitsBothKnownLengthAndStreamingBodies() throws Exception {
        mvc.perform(registration(" ".repeat(4097))).andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("BODY_TOO_LARGE"));
        verifyNoInteractions(service);
    }

    @Test
    void streamingBodyWithoutContentLengthIsStillBounded() throws Exception {
        var request = new org.springframework.mock.web.MockHttpServletRequest() {
            @Override public long getContentLengthLong() { return -1; }
        };
        request.setRequestURI("/api/locations");
        request.setMethod("POST");
        request.addHeader("Host", "127.0.0.1:8080");
        request.addHeader("X-Flight-Monitor", "1");
        request.setContent(new byte[4097]);
        var response = new org.springframework.mock.web.MockHttpServletResponse();
        var chain = new org.springframework.mock.web.MockFilterChain();
        new LocalApiProtection(new LifecycleSettings(false, null, null, null, 0, 0, 0))
                .doFilter(request, response, chain);
        org.assertj.core.api.Assertions.assertThat(response.getStatus()).isEqualTo(400);
        org.assertj.core.api.Assertions.assertThat(response.getContentAsString()).contains("INVALID_BODY");
        org.assertj.core.api.Assertions.assertThat(chain.getRequest()).isNull();
    }

    @Test
    void rejectsCrossOriginSimpleRequestsAndDnsRebinding() throws Exception {
        mvc.perform(registration("{}").header("Origin", "https://attacker.example")).andExpect(status().isForbidden());
        mvc.perform(registration("{}").header("Sec-Fetch-Site", "cross-site")).andExpect(status().isForbidden());
        mvc.perform(post("/api/locations").header("Host", "127.0.0.1:8080")
                .contentType(MediaType.APPLICATION_JSON).content("{}")).andExpect(status().isForbidden());
        mvc.perform(post("/api/locations").header("Host", "attacker.example")
                .header("X-Flight-Monitor", "1").content("{}")).andExpect(status().isForbidden());
        for (String path : new String[]{"/%61pi/locations", "/api;/locations"}) {
            mvc.perform(post(path).header("Host", "attacker.example")
                    .header("X-Flight-Monitor", "1").contentType(MediaType.APPLICATION_JSON).content("{}"))
                    .andExpect(status().isForbidden());
        }
        mvc.perform(options("/api/locations").header("Host", "127.0.0.1:8080")
                .header("Origin", "https://attacker.example")).andExpect(status().isForbidden())
                .andExpect(header().doesNotExist("Access-Control-Allow-Origin"));
        verifyNoInteractions(service);
    }

    @Test
    void uncertainWriteContainsOperationNotRollbackAndDeletionIs202() throws Exception {
        when(service.register("key", 0, 0)).thenThrow(new LifecycleFailure(
                503, "OUTCOME_UNCERTAIN", "Write may still commit", ID));
        mvc.perform(registration("{\"latitude\":0,\"longitude\":0}"))
                .andExpect(status().isServiceUnavailable()).andExpect(jsonPath("$.operationId").value(ID))
                .andExpect(jsonPath("$.outcome").value("UNCERTAIN"));
        when(service.delete(ID)).thenReturn(new LifecycleService.Deletion(ID, ID, true));
        mvc.perform(delete("/api/locations/" + ID).header("Host", "127.0.0.1:8080")
                .header("Origin", "http://127.0.0.1:8080").header("X-Flight-Monitor", "1"))
                .andExpect(status().isAccepted()).andExpect(jsonPath("$.cleanupPending").value(true));
    }
}
