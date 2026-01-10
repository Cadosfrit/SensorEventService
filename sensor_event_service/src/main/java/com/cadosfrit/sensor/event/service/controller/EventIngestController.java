package com.cadosfrit.sensor.event.service.controller;

import com.cadosfrit.sensor.event.service.dto.EventRequestDTO;
import com.cadosfrit.sensor.event.service.dto.IngestResponseDTO;
import com.cadosfrit.sensor.event.service.service.EventIngestService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/events")
public class EventIngestController {

    private final EventIngestService ingestService;
    private final EventIngestService ingestServiceV2;

    public EventIngestController(
            @Qualifier("EventIngestServiceV1") EventIngestService ingestService,
            @Qualifier("EventIngestServiceV2") EventIngestService ingestServiceV2
    ) {
        this.ingestService = ingestService;
        this.ingestServiceV2 = ingestServiceV2;
    }

    @PostMapping("/batch")
    public ResponseEntity<?> ingestBatch(@RequestBody List<EventRequestDTO> batch) {
        try {
            if (batch == null || batch.isEmpty()) {
                log.warn("Invalid request: batch is null or empty");
                return ResponseEntity.badRequest().body("Batch cannot be empty");
            }

            if (batch.size() > 10000) {
                log.warn("Invalid request: batch size {} exceeds maximum limit", batch.size());
                return ResponseEntity.badRequest().body("Batch size cannot exceed 10000 events");
            }

            for (int i = 0; i < batch.size(); i++) {
                EventRequestDTO event = batch.get(i);
                if (event == null) {
                    log.warn("Invalid request: event at index {} is null", i);
                    return ResponseEntity.badRequest().body("Event at index " + i + " is null");
                }

                if (event.getEventId() == null || event.getEventId().trim().isEmpty()) {
                    log.warn("Invalid request: event at index {} has null or empty eventId", i);
                    return ResponseEntity.badRequest().body("Event at index " + i + " must have an eventId");
                }

                if (event.getMachineId() == null || event.getMachineId().trim().isEmpty()) {
                    log.warn("Invalid request: event at index {} has null or empty machineId", i);
                    return ResponseEntity.badRequest().body("Event at index " + i + " must have a machineId");
                }
            }

            log.info("Received ingestion batch with size: {}", batch.size());
            IngestResponseDTO response = ingestService.processBatch(batch);

            if (response == null) {
                log.error("Service returned null response for batch");
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body("Error processing batch");
            }

            log.info("Batch processed. Accepted: {}, Rejected: {}", response.getAccepted(), response.getRejected());
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error processing batch", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error processing batch: " + e.getMessage());
        }
    }

    @PostMapping("/v2/batch")
    public ResponseEntity<?> ingestBatchV2(@RequestBody List<EventRequestDTO> batch) {
        try {
            if (batch == null || batch.isEmpty()) {
                log.warn("Invalid request: v2 batch is null or empty");
                return ResponseEntity.badRequest().body("Batch cannot be empty");
            }

            if (batch.size() > 10000) {
                log.warn("Invalid request: v2 batch size {} exceeds maximum limit", batch.size());
                return ResponseEntity.badRequest().body("Batch size cannot exceed 10000 events");
            }

            for (int i = 0; i < batch.size(); i++) {
                EventRequestDTO event = batch.get(i);
                if (event == null) {
                    log.warn("Invalid request: v2 event at index {} is null", i);
                    return ResponseEntity.badRequest().body("Event at index " + i + " is null");
                }

                if (event.getEventId() == null || event.getEventId().trim().isEmpty()) {
                    log.warn("Invalid request: v2 event at index {} has null or empty eventId", i);
                    return ResponseEntity.badRequest().body("Event at index " + i + " must have an eventId");
                }

                if (event.getMachineId() == null || event.getMachineId().trim().isEmpty()) {
                    log.warn("Invalid request: v2 event at index {} has null or empty machineId", i);
                    return ResponseEntity.badRequest().body("Event at index " + i + " must have a machineId");
                }
            }

            log.info("V2: Received ingestion batch with size: {}", batch.size());
            IngestResponseDTO response = ingestServiceV2.processBatch(batch);

            if (response == null) {
                log.error("Service returned null response for v2 batch");
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body("Error processing batch");
            }

            log.info("V2: Batch processed. Accepted: {}, Updated: {}, Deduped: {}",
                    response.getAccepted(), response.getUpdated(), response.getDeduped());
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error processing v2 batch", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error processing batch: " + e.getMessage());
        }
    }
}