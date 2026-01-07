package com.cadosfrit.sensor_event_service.constants;

import lombok.Getter;

@Getter
public enum Constants {

    // Validation Error Codes
    INVALID_DURATION("INVALID_DURATION"),
    FUTURE_EVENT_TIME("FUTURE_EVENT_TIME"),

    // Ingest Status Codes
    ACCEPTED("ACCEPTED"),
    UPDATED("UPDATED"),
    DEDUPED("DEDUPED"),

    WARNING("Warning"),
    HEALTHY("Healthy"),

    STATUS("status"),
    COUNT("count");

    private final String code;

    Constants(String code) {
        this.code = code;
    }
}