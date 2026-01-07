package com.cadosfrit.sensor_event_service.constants;

import lombok.Getter;

@Getter
public enum Constants {

    // Validation Error Codes
    INVALID_DURATION("INVALID_DURATION"),
    FUTURE_EVENT_TIME("FUTURE_EVENT_TIME"),

    // Ingest Status Codes
    ACCEPTED("accepted"),
    UPDATED("updated"),
    DEDUPED("deduped"),

    WARNING("Warning"),
    HEALTHY("Healthy");

    private final String code;

    Constants(String code) {
        this.code = code;
    }
}