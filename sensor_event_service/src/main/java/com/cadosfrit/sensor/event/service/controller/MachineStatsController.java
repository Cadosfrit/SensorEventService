package com.cadosfrit.sensor.event.service.controller;

import com.cadosfrit.sensor.event.service.dto.MachineStatsDTO;
import com.cadosfrit.sensor.event.service.service.MachineStatsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/stats")
@RequiredArgsConstructor
public class MachineStatsController {

    private final MachineStatsService statsService;

    @GetMapping
    public ResponseEntity<?> getMachineStats(
            @RequestParam String machineId,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant end) {

        try {
            if (machineId == null || machineId.trim().isEmpty()) {
                log.warn("Invalid request: machineId is null or empty");
                return ResponseEntity.badRequest().body("Machine ID is required");
            }

            if (start == null || end == null) {
                log.warn("Invalid request: start or end time is null");
                return ResponseEntity.badRequest().body("Start and end times are required");
            }

            if (!start.isBefore(end)) {
                log.warn("Invalid request: start time {} is not before end time {}", start, end);
                return ResponseEntity.badRequest().body("Start time must be before end time");
            }

            log.debug("Fetching stats for machine: {} from {} to {}", machineId, start, end);
            MachineStatsDTO result = statsService.getMachineStats(machineId, start, end);

            if (result == null) {
                log.warn("No stats available for machineId: {}", machineId);
                return ResponseEntity.noContent().build();
            }

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            log.error("Error fetching stats for machineId: {}", machineId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error fetching machine statistics");
        }
    }

    @GetMapping("/top-defect-lines")
    public ResponseEntity<?> getTopDefectLines(
            @RequestParam String factoryId,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(defaultValue = "10") int limit) {

        try {
            if (factoryId == null || factoryId.trim().isEmpty()) {
                log.warn("Invalid request: factoryId is null or empty");
                return ResponseEntity.badRequest().body("Factory ID is required");
            }

            if (from == null || to == null) {
                log.warn("Invalid request: from or to time is null");
                return ResponseEntity.badRequest().body("From and to times are required");
            }

            if (!from.isBefore(to)) {
                log.warn("Invalid request: from time {} is not before to time {}", from, to);
                return ResponseEntity.badRequest().body("From time must be before to time");
            }

            if (limit <= 0) {
                log.warn("Invalid request: limit {} is not positive", limit);
                return ResponseEntity.badRequest().body("Limit must be greater than 0");
            }

            log.debug("Fetching top {} defect lines for factory: {} from {} to {}", limit, factoryId, from, to);
            List<Map<String, Object>> result = statsService.getTopDefectLines(factoryId, from, to, limit);

            if (result == null || result.isEmpty()) {
                log.debug("No defect lines found for factoryId: {}", factoryId);
                return ResponseEntity.ok(List.of());
            }

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            log.error("Error fetching defect lines for factoryId: {}", factoryId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error fetching top defect lines");
        }
    }
}