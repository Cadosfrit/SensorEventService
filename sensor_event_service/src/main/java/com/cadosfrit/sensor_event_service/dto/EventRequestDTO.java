package com.cadosfrit.sensor_event_service.dto;

import lombok.Data;
import java.time.Instant;

@Data
public class EventRequestDTO {
    private String eventId;
    private String machineId;
    private Instant eventTime;
    private Instant receivedTime;
    private Long durationMs;
    private Integer defectCount;
}