package com.cadosfrit.sensor_event_service.controller;

import com.cadosfrit.sensor_event_service.dto.EventRequestDTO;
import com.cadosfrit.sensor_event_service.dto.IngestResponseDTO;
import com.cadosfrit.sensor_event_service.service.EventIngestService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
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
    public ResponseEntity<IngestResponseDTO> ingestBatch(@RequestBody List<EventRequestDTO> batch) {
        log.info("Received ingestion batch with size: {}", batch.size());
        IngestResponseDTO response = ingestService.processBatch(batch);
        log.info("Batch processed. Accepted: {}, Rejected: {}", response.getAccepted(), response.getRejected());
        return ResponseEntity.ok(response);
    }
    @PostMapping("/v2/batch")
    public ResponseEntity<IngestResponseDTO> ingestBatchV2(@RequestBody List<EventRequestDTO> batch) {
        log.info("V2: Received ingestion batch with size: {}", batch.size());
        IngestResponseDTO response = ingestServiceV2.processBatch(batch);
        log.info("V2: Batch processed. Accepted: {}, Updated: {}, Deduped: {}",
                response.getAccepted(), response.getUpdated(), response.getDeduped());
        return ResponseEntity.ok(response);
    }
}