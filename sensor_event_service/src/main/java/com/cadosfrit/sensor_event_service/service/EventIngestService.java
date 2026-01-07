package com.cadosfrit.sensor_event_service.service;

import com.cadosfrit.sensor_event_service.dto.EventRequestDTO;
import com.cadosfrit.sensor_event_service.dto.IngestResponseDTO;
import java.util.List;

public interface EventIngestService {
    /**
     * Processes a batch of events, validates them, 
     * and performs high-performance persistence.
     */
    IngestResponseDTO processBatch(List<EventRequestDTO> batch);
}
