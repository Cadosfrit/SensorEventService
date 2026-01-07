package com.cadosfrit.sensor_event_service.service.impl;

import com.cadosfrit.sensor_event_service.constants.Constants;
import com.cadosfrit.sensor_event_service.dto.EventRequestDTO;
import com.cadosfrit.sensor_event_service.dto.IngestResponseDTO;
import com.cadosfrit.sensor_event_service.repository.MachineEventRepository;
import com.cadosfrit.sensor_event_service.service.EventIngestService;
import com.cadosfrit.sensor_event_service.stratergy.EventValidationStrategy;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Service
@RequiredArgsConstructor
public class EventIngestServiceImpl implements EventIngestService {

    private final ObjectMapper objectMapper;
    private final MachineEventRepository repository;
    private final List<EventValidationStrategy> validationStrategies;

    @Override
    @Transactional
    public IngestResponseDTO processBatch(List<EventRequestDTO> batch) {
        BatchDedupResult dedupResult = deduplicateInBatch(batch);

        ValidationResult validationResult = validateEvents(dedupResult.uniqueEvents);

        DbPersistResult dbResult = persistBatch(validationResult.validEvents);

        return IngestResponseDTO.builder()
                .accepted(dbResult.accepted)
                .updated(dbResult.updated)
                .deduped(dedupResult.duplicateCount + dbResult.dbDeduped)
                .rejected(validationResult.rejections.size())
                .rejections(validationResult.rejections)
                .build();
    }

    private BatchDedupResult deduplicateInBatch(List<EventRequestDTO> batch) {
        Map<String, EventRequestDTO> uniqueMap = new LinkedHashMap<>();
        int duplicates = 0;

        for (EventRequestDTO event : batch) {
            if (uniqueMap.containsKey(event.getEventId())) {
                duplicates++;
            } else {
                uniqueMap.put(event.getEventId(), event);
            }
        }
        return new BatchDedupResult(new ArrayList<>(uniqueMap.values()), duplicates);
    }

    private ValidationResult validateEvents(List<EventRequestDTO> uniqueEvents) {
        List<EventRequestDTO> validEvents = new ArrayList<>();
        List<IngestResponseDTO.Rejection> rejections = new ArrayList<>();

        for (EventRequestDTO event : uniqueEvents) {
            Optional<String> error = runValidations(event);
            if (error.isPresent()) {
                rejections.add(new IngestResponseDTO.Rejection(event.getEventId(), error.get()));
            } else {
                validEvents.add(event);
            }
        }
        return new ValidationResult(validEvents, rejections);
    }

    private DbPersistResult persistBatch(List<EventRequestDTO> validEvents) {
        if (validEvents.isEmpty()) {
            return new DbPersistResult(0, 0, 0);
        }

        try {
            String json = objectMapper.writeValueAsString(validEvents);
            Map<String, Integer> counts = repository.processBatchInDB(json);

            int accepted = counts.getOrDefault(Constants.ACCEPTED.getCode(), 0);
            int updated = counts.getOrDefault(Constants.UPDATED.getCode(), 0);
            int deduped = counts.getOrDefault(Constants.DEDUPED.getCode(), 0);

            return new DbPersistResult(accepted, updated, deduped);

        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error serializing event batch for database persistence", e);
        }
    }

    private Optional<String> runValidations(EventRequestDTO event) {
        return validationStrategies.stream()
                .map(s -> s.validate(event))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    private record BatchDedupResult(List<EventRequestDTO> uniqueEvents, int duplicateCount) {}

    private record ValidationResult(List<EventRequestDTO> validEvents, List<IngestResponseDTO.Rejection> rejections) {}

    private record DbPersistResult(int accepted, int updated, int dbDeduped) {}
}