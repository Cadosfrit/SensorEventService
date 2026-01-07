package com.cadosfrit.sensor_event_service.service;

import com.cadosfrit.sensor_event_service.dto.MachineStatsDTO;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public interface MachineStatsService {
    /**
     * Calculates stats for a specific machine in a time window.
     */
    MachineStatsDTO getMachineStats(String machineId, Instant start, Instant end);

    /**
     * Returns top production lines with the highest defect rates.
     */
    List<Map<String, Object>> getTopDefectLines(String factoryId, Instant from, Instant to, int limit);
}
