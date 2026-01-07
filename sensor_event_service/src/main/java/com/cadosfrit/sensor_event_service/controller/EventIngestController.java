package com.cadosfrit.sensor_event_service.controller;

import com.cadosfrit.sensor_event_service.dto.EventRequestDTO;
import com.cadosfrit.sensor_event_service.dto.IngestResponseDTO;
import com.cadosfrit.sensor_event_service.service.EventIngestService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/events")
@RequiredArgsConstructor
public class EventIngestController {

    private final EventIngestService ingestService;

    @PostMapping("/batch")
    public ResponseEntity<IngestResponseDTO> ingestBatch(@RequestBody List<EventRequestDTO> batch) {
        log.info("Received ingestion batch with size: {}", batch.size());

        IngestResponseDTO response = ingestService.processBatch(batch);

        log.info("Batch processed. Accepted: {}, Rejected: {}", response.getAccepted(), response.getRejected());
        return ResponseEntity.ok(response);
    }
}