package com.cadosfrit.sensor.event.service.service.impl;

import com.cadosfrit.sensor.event.service.constants.Constants;
import com.cadosfrit.sensor.event.service.dto.MachineStatsDTO;
import com.cadosfrit.sensor.event.service.repository.MachineEventRepository;
import com.cadosfrit.sensor.event.service.service.MachineStatsService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

@Service
@RequiredArgsConstructor
public class MachineStatsServiceImpl implements MachineStatsService {

    private static final Logger logger = LoggerFactory.getLogger(MachineStatsServiceImpl.class);
    private final MachineEventRepository eventRepository;

    @Override
    public MachineStatsDTO getMachineStats(String machineId, Instant start, Instant end) {
        try {
            if (machineId == null || machineId.isEmpty() || start == null || end == null) {
                return buildDefaultStats(machineId, start, end);
            }

            if (!start.isBefore(end)) {
                return buildDefaultStats(machineId, start, end);
            }

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

        } catch (Exception e) {
            logger.error("Error in getMachineStats for machineId: {}", machineId, e);
            return buildDefaultStats(machineId, start, end);
        }
    }

    @Override
    public List<Map<String, Object>> getTopDefectLines(String factoryId, Instant from, Instant to, int limit) {
        try {
            if (factoryId == null || factoryId.isEmpty() || from == null || to == null || limit <= 0) {
                return Collections.emptyList();
            }

            if (!from.isBefore(to)) {
                return Collections.emptyList();
            }

            List<Object[]> results = eventRepository.findTopDefectLinesRaw(factoryId, from, to, limit);

            if (results == null || results.isEmpty()) {
                return Collections.emptyList();
            }

            List<Map<String, Object>> responseList = new ArrayList<>();
            for (Object[] row : results) {
                if (row != null && row.length > 0) {
                    try {
                        responseList.add(mapLineStatsRow(row));
                    } catch (Exception e) {
                        logger.warn("Error mapping row for factoryId: {}", factoryId, e);
                    }
                }
            }
            return responseList;

        } catch (Exception e) {
            logger.error("Error in getTopDefectLines for factoryId: {}", factoryId, e);
            return Collections.emptyList();
        }
    }

    /**
     * Safe parsing of DB result which might be null if no data exists.
     */
    private StatsRawData parseRawStats(Object result) {
        try {
            long defects = 0;
            long events = 0;

            if (result == null) {
                return new StatsRawData(defects, events);
            }

            Object[] row = null;
            try {
                Object[] tempRow = (Object[]) result;
                row = (tempRow.length > 0 && tempRow[0] instanceof Object[])
                    ? (Object[]) tempRow[0]
                    : tempRow;
            } catch (ClassCastException e) {
                logger.warn("Error parsing stats result", e);
                return new StatsRawData(defects, events);
            }

            if (row != null && row.length > 0 && row[0] != null) {
                try {
                    defects = ((Number) row[0]).longValue();
                } catch (ClassCastException e) {
                    logger.warn("Error parsing defects value", e);
                }
            }

            if (row != null && row.length > 1 && row[1] != null) {
                try {
                    events = ((Number) row[1]).longValue();
                } catch (ClassCastException e) {
                    logger.warn("Error parsing events value", e);
                }
            }

            return new StatsRawData(defects, events);

        } catch (Exception e) {
            logger.error("Error in parseRawStats", e);
            return new StatsRawData(0, 0);
        }
    }

    private double calculateWindowHours(Instant start, Instant end) {
        try {
            if (start == null || end == null) {
                return 0.0;
            }

            long durationSeconds = Duration.between(start, end).getSeconds();
            return durationSeconds < 0 ? 0.0 : durationSeconds / 3600.0;

        } catch (ArithmeticException e) {
            logger.error("Error calculating window hours", e);
            return 0.0;
        } catch (Exception e) {
            logger.error("Error calculating window hours", e);
            return 0.0;
        }
    }

    private double calculateDefectRate(long defects, double windowHours) {
        try {
            if (windowHours <= 0 || defects < 0) {
                return 0.0;
            }

            return defects / windowHours;

        } catch (ArithmeticException e) {
            logger.error("Error calculating defect rate", e);
            return 0.0;
        } catch (Exception e) {
            logger.error("Error calculating defect rate", e);
            return 0.0;
        }
    }

    private Map<String, Object> mapLineStatsRow(Object[] row) {
        try {
            if (row == null || row.length < 3) {
                return new LinkedHashMap<>();
            }

            String lineId = "UNKNOWN";
            long totalDefects = 0;
            long eventCount = 0;

            try {
                lineId = (String) row[0];
                if (lineId == null || lineId.isEmpty()) {
                    lineId = "UNKNOWN";
                }
            } catch (ClassCastException e) {
                logger.warn("Error casting lineId", e);
            }

            try {
                totalDefects = (row[1] != null) ? ((Number) row[1]).longValue() : 0;
            } catch (ClassCastException e) {
                logger.warn("Error parsing totalDefects", e);
            }

            try {
                eventCount = (row[2] != null) ? ((Number) row[2]).longValue() : 0;
            } catch (ClassCastException e) {
                logger.warn("Error parsing eventCount", e);
            }

            double defectsPercent = (eventCount > 0) ? ((double) totalDefects / eventCount) * 100.0 : 0.0;

            Map<String, Object> map = new LinkedHashMap<>();
            map.put("lineId", lineId);
            map.put("totalDefects", totalDefects);
            map.put("eventCount", eventCount);
            map.put("defectsPercent", roundToTwoDecimals(defectsPercent));

            return map;

        } catch (Exception e) {
            logger.error("Error in mapLineStatsRow", e);
            return new LinkedHashMap<>();
        }
    }

    private double roundToTwoDecimals(double value) {
        try {
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                return 0.0;
            }
            return Math.round(value * 100.0) / 100.0;

        } catch (Exception e) {
            logger.error("Error in roundToTwoDecimals", e);
            return 0.0;
        }
    }

    /**
     * Build a default stats object when processing fails
     */
    private MachineStatsDTO buildDefaultStats(String machineId, Instant start, Instant end) {
        try {
            return MachineStatsDTO.builder()
                    .machineId(machineId != null ? machineId : "UNKNOWN")
                    .start(start)
                    .end(end)
                    .eventsCount(0)
                    .defectsCount(0)
                    .avgDefectRate(0.0)
                    .status(Constants.HEALTHY.getCode())
                    .build();
        } catch (Exception e) {
            logger.error("Error building default stats", e);
            return null;
        }
    }

    private record StatsRawData(long defects, long events) {}
}