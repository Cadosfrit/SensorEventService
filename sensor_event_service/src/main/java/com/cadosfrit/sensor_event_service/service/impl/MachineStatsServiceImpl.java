package com.cadosfrit.sensor_event_service.service.impl;

import com.cadosfrit.sensor_event_service.constants.Constants;
import com.cadosfrit.sensor_event_service.dto.MachineStatsDTO;
import com.cadosfrit.sensor_event_service.repository.MachineEventRepository;
import com.cadosfrit.sensor_event_service.repository.ProductionLineRepository;
import com.cadosfrit.sensor_event_service.service.MachineStatsService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

@Service
@RequiredArgsConstructor
public class MachineStatsServiceImpl implements MachineStatsService {

    private final MachineEventRepository eventRepository;
    private final ProductionLineRepository lineRepository;

    @Override
    public MachineStatsDTO getMachineStats(String machineId, Instant start, Instant end) {

        Object result = eventRepository.getMachineStatsRaw(machineId, start, end);

        StatsRawData rawData = parseRawStats(result);

        double windowHours = calculateWindowHours(start, end);
        double avgDefectRate = calculateDefectRate(rawData.defects, windowHours);

        String status = (avgDefectRate < 2.0) ? Constants.HEALTHY.getCode() : Constants.WARNING.getCode();

        return MachineStatsDTO.builder()
                .machineId(machineId)
                .start(start)
                .end(end)
                .eventsCount(rawData.events)
                .defectsCount(rawData.defects)
                .avgDefectRate(roundToTwoDecimals(avgDefectRate))
                .status(status)
                .build();
    }

    @Override
    public List<Map<String, Object>> getTopDefectLines(String factoryId, Instant from, Instant to, int limit) {
        List<Object[]> results = lineRepository.findTopDefectLinesRaw(factoryId, from, to, limit);

        if (results == null || results.isEmpty()) {
            return Collections.emptyList();
        }

        List<Map<String, Object>> responseList = new ArrayList<>();

        for (Object[] row : results) {
            responseList.add(mapLineStatsRow(row));
        }

        return responseList;
    }

    /**
     * Safe parsing of DB result which might be null if no data exists.
     */
    private StatsRawData parseRawStats(Object result) {
        long defects = 0;
        long events = 0;

        if (result != null) {

            Object[] row = (Object[]) result;

            defects = (row[0] != null) ? ((Number) row[0]).longValue() : 0;
            events = (row[1] != null) ? ((Number) row[1]).longValue() : 0;
        }
        return new StatsRawData(defects, events);
    }

    private double calculateWindowHours(Instant start, Instant end) {
        long durationSeconds = Duration.between(start, end).getSeconds();
        return durationSeconds / 3600.0;
    }

    private double calculateDefectRate(long defects, double windowHours) {
        if (windowHours <= 0) return 0.0;
        return defects / windowHours;
    }

    private Map<String, Object> mapLineStatsRow(Object[] row) {
        String lineId = (String) row[0];
        long totalDefects = (row[1] != null) ? ((Number) row[1]).longValue() : 0;
        long eventCount = (row[2] != null) ? ((Number) row[2]).longValue() : 0;

        double defectsPercent = 0.0;
        if (eventCount > 0) {
            defectsPercent = ((double) totalDefects / eventCount) * 100.0;
        }

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("lineId", lineId);
        map.put("totalDefects", totalDefects);
        map.put("eventCount", eventCount);
        map.put("defectsPercent", roundToTwoDecimals(defectsPercent));

        return map;
    }

    private double roundToTwoDecimals(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    private record StatsRawData(long defects, long events) {}
}