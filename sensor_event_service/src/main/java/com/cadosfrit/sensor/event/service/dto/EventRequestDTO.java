package com.cadosfrit.sensor.event.service.dto;

import lombok.Data;
import java.time.Instant;

@Data
public class EventRequestDTO {
    private String eventId;
    private String machineId;
    private Instant eventTime;
    private Instant receivedTime;
    private long durationMs;
    private int defectCount;
}