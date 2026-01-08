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
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;

@Service("EventIngestServiceV2")
@RequiredArgsConstructor
@Slf4j
public class EventIngestServiceV2Impl implements EventIngestService {

    private final ObjectMapper objectMapper;
    private final MachineEventRepository repository;
    private final List<EventValidationStrategy> validationStrategies;

    @Override
    @Transactional
    public IngestResponseDTO processBatch(List<EventRequestDTO> batch) {
        ValidationResult validationResult = validateEvents(batch);

        if (validationResult.validEvents.isEmpty()) {
            return buildResponse(new DbPersistResult(0,0,0), validationResult);
        }

        SplitBatchResult splitResult = splitBatch(validationResult.validEvents);

        DbPersistResult fastResult = persistFastBatch(splitResult.fastBatch);

        DbPersistResult slowResult = persistSlowBatch(splitResult.slowBatch);

        DbPersistResult totalResult = mergeResults(fastResult, slowResult);

        return buildResponse(totalResult, validationResult);
    }

    private SplitBatchResult splitBatch(List<EventRequestDTO> events) {
        Map<String, Integer> frequencyMap = new HashMap<>();
        for (EventRequestDTO event : events) {
            frequencyMap.merge(event.getEventId(), 1, Integer::sum);
        }

        List<EventRequestDTO> fastBatch = new ArrayList<>();
        List<EventRequestDTO> slowBatch = new ArrayList<>();

        for (EventRequestDTO event : events) {
            if (frequencyMap.get(event.getEventId()) > 1) {
                slowBatch.add(event);
            } else {
                fastBatch.add(event);
            }
        }
        return new SplitBatchResult(fastBatch, slowBatch);
    }

    private DbPersistResult persistFastBatch(List<EventRequestDTO> events) {
        if (events.isEmpty()) return new DbPersistResult(0, 0, 0);
        try {
            String json = convertToDbJson(events);
            List<Map<String, Object>> rows = repository.processFastBatchSP(json);
            return parseDbStats(rows);
        } catch (Exception e) {
            throw new RuntimeException("Fast Batch Persistence Failed", e);
        }
    }

    private DbPersistResult persistSlowBatch(List<EventRequestDTO> events) {
        if (events.isEmpty()) return new DbPersistResult(0, 0, 0);
        try {
            String json = convertToDbJson(events);
            List<Map<String, Object>> rows = repository.processSlowBatchSP(json);
            return parseDbStats(rows);
        } catch (Exception e) {
            throw new RuntimeException("Slow Batch Persistence Failed", e);
        }
    }

    private String convertToDbJson(List<EventRequestDTO> events) throws JsonProcessingException {
        List<Map<String, Object>> dbRows = new ArrayList<>();
        Instant now = Instant.now();

        for (EventRequestDTO event : events) {
            Map<String, Object> row = new HashMap<>();
            row.put("event_id", event.getEventId());
            row.put("machine_id", event.getMachineId());
            row.put("event_time", event.getEventTime());
            row.put("received_time", now);
            row.put("defect_count", event.getDefectCount());
            row.put("duration_ms", event.getDurationMs());
            dbRows.add(row);
        }
        return objectMapper.writeValueAsString(dbRows);
    }

    private DbPersistResult parseDbStats(List<Map<String, Object>> dbRows) {
        int accepted = 0;
        int updated = 0;
        int deduped = 0;

        for (Map<String, Object> row : dbRows) {
            String status = (String) row.get("status");
            Number countNum = (Number) row.get("count");
            int count = (countNum != null) ? countNum.intValue() : 0;

            if (Constants.ACCEPTED.getCode().equals(status)) accepted += count;
            else if (Constants.UPDATED.getCode().equals(status)) updated += count;
            else if (Constants.DEDUPED.getCode().equals(status)) deduped += count;
        }
        return new DbPersistResult(accepted, updated, deduped);
    }

    private DbPersistResult mergeResults(DbPersistResult r1, DbPersistResult r2) {
        return new DbPersistResult(
                r1.accepted + r2.accepted,
                r1.updated + r2.updated,
                r1.deduped + r2.deduped
        );
    }

    private ValidationResult validateEvents(List<EventRequestDTO> batch) {
        List<EventRequestDTO> validEvents = new ArrayList<>();
        List<IngestResponseDTO.Rejection> rejections = new ArrayList<>();

        for (EventRequestDTO event : batch) {
            Optional<String> error = runValidations(event);
            if (error.isPresent()) {
                rejections.add(new IngestResponseDTO.Rejection(event.getEventId(), error.get()));
            } else {
                validEvents.add(event);
            }
        }
        return new ValidationResult(validEvents, rejections);
    }

    private Optional<String> runValidations(EventRequestDTO event) {
        return validationStrategies.stream()
                .map(s -> s.validate(event))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    private IngestResponseDTO buildResponse(DbPersistResult dbResult, ValidationResult validationResult) {
        return IngestResponseDTO.builder()
                .accepted(dbResult.accepted)
                .updated(dbResult.updated)
                .deduped(dbResult.deduped)
                .rejected(validationResult.rejections.size())
                .rejections(validationResult.rejections)
                .build();
    }

    private record ValidationResult(List<EventRequestDTO> validEvents, List<IngestResponseDTO.Rejection> rejections) {}
    private record SplitBatchResult(List<EventRequestDTO> fastBatch, List<EventRequestDTO> slowBatch) {}
    private record DbPersistResult(int accepted, int updated, int deduped) {}
}