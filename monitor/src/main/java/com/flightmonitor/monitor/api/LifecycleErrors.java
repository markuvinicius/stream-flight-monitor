package com.flightmonitor.monitor.api;

import com.flightmonitor.monitor.lifecycle.LifecycleFailure;
import org.springframework.http.*;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.bind.annotation.*;

/** Stable, coordinate-free problem responses. Unexpected failures are not converted into success. */
@RestControllerAdvice
public final class LifecycleErrors {
    @ExceptionHandler(LifecycleFailure.class)
    public ResponseEntity<ProblemDetail> lifecycle(LifecycleFailure failure) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatusCode.valueOf(failure.status()), failure.getMessage());
        problem.setTitle(failure.code());
        problem.setProperty("code", failure.code());
        if (failure.operationId() != null) {
            problem.setProperty("operationId", failure.operationId());
        }
        if (failure.code().equals("OUTCOME_UNCERTAIN")) {
            problem.setProperty("outcome", "UNCERTAIN");
        }
        return ResponseEntity.status(failure.status()).contentType(MediaType.APPLICATION_PROBLEM_JSON).body(problem);
    }

    @ExceptionHandler({HttpMessageNotReadableException.class, HttpMediaTypeNotSupportedException.class})
    public ResponseEntity<ProblemDetail> malformed(Exception exception) {
        return lifecycle(new LifecycleFailure(400, "INVALID_JSON", "A valid, bounded JSON body is required", null));
    }
}
