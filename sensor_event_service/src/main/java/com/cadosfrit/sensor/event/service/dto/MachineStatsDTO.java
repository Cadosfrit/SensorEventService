package com.cadosfrit.sensor.event.service.dto;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;

@Data
@Builder
public class MachineStatsDTO {
    private Instant start;
    private Instant end;
    private String machineId;
    private long eventsCount;
    private long defectsCount;
    private double avgDefectRate;
    private String status;
}