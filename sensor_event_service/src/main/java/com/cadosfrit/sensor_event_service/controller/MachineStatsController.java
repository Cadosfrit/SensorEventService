package com.cadosfrit.sensor_event_service.controller;

import com.cadosfrit.sensor_event_service.dto.MachineStatsDTO;
import com.cadosfrit.sensor_event_service.service.MachineStatsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
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
    public ResponseEntity<MachineStatsDTO> getMachineStats(
            @RequestParam String machineId,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant end) {

        log.debug("Fetching stats for machine: {} from {} to {}", machineId, start, end);
        return ResponseEntity.ok(statsService.getMachineStats(machineId, start, end));
    }

    @GetMapping("/top-defect-lines")
    public ResponseEntity<List<Map<String, Object>>> getTopDefectLines(
            @RequestParam String factoryId,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(defaultValue = "10") int limit) {

        log.debug("Fetching top {} defect lines for factory: {} from {} to {}", limit, factoryId, from, to);
        return ResponseEntity.ok(statsService.getTopDefectLines(factoryId, from, to, limit));
    }
}