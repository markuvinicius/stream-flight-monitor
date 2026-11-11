package com.flightmonitor.monitor.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.flightmonitor.contracts.event.LifecycleWire;
import com.flightmonitor.monitor.lifecycle.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

import java.net.URI;

/** Task-3 lifecycle and operation APIs; tracking and aircraft queries are separate later tasks. */
@RestController
@RequestMapping("/api")
public final class LifecycleController {
    private final LifecycleService service;

    public LifecycleController(LifecycleService service) {
        this.service = service;
    }

    @PostMapping(value = "/locations", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<LifecycleService.Registration> register(
            @RequestHeader(value = "Idempotency-Key", required = false) String key,
            @RequestBody JsonNode body) {
        if (!body.isObject() || body.size() != 2 || !body.has("latitude") || !body.has("longitude")
                || !body.get("latitude").isNumber() || !body.get("longitude").isNumber()) {
            throw new LifecycleFailure(400, "INVALID_COORDINATES",
                    "Expected exactly numeric latitude and longitude", null);
        }
        var receipt = service.register(key, body.get("latitude").doubleValue(), body.get("longitude").doubleValue());
        return ResponseEntity.created(URI.create("/api/locations/" + receipt.locationId())).body(receipt);
    }

    @GetMapping("/operations/{id}")
    public LifecycleWire.Operation operation(@PathVariable String id) {
        return service.operation(id);
    }

    @DeleteMapping("/locations/{id}")
    public ResponseEntity<LifecycleService.Deletion> delete(@PathVariable String id) {
        return ResponseEntity.accepted().body(service.delete(id));
    }
}
