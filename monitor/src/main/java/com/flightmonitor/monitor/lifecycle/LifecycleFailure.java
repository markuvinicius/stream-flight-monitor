package com.flightmonitor.monitor.lifecycle;

/** Structured API error; an uncertain write is never represented as a rollback. */
public final class LifecycleFailure extends RuntimeException {
    private final int status;
    private final String code;
    private final String operationId;

    public LifecycleFailure(int status, String code, String message, String operationId) {
        super(message);
        this.status = status;
        this.code = code;
        this.operationId = operationId;
    }

    public int status() { return status; }
    public String code() { return code; }
    public String operationId() { return operationId; }
}
