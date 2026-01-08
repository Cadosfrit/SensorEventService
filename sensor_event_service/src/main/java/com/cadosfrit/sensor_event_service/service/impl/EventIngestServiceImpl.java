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

import java.time.Instant;
import java.util.*;

@Service("EventIngestServiceV1")
@RequiredArgsConstructor
public class EventIngestServiceImpl implements EventIngestService {

    private final ObjectMapper objectMapper;
    private final MachineEventRepository repository;
    private final List<EventValidationStrategy> validationStrategies;

    @Override
    @Transactional
    public IngestResponseDTO processBatch(List<EventRequestDTO> batch) {
        BatchPreProcessResult preProcessResult = preprocessBatch(batch);

        ValidationResult validationResult = validateEvents(preProcessResult.sanitizedList);

        DbPersistResult dbResult = persistBatch(validationResult.validEvents,
                preProcessResult.intraUpdates,
                preProcessResult.intraDedups);

        return IngestResponseDTO.builder()
                .accepted(dbResult.accepted)
                .updated(dbResult.updated)
                .deduped(dbResult.dbDeduped)
                .rejected(validationResult.rejections.size())
                .rejections(validationResult.rejections)
                .build();
    }

    private BatchPreProcessResult preprocessBatch(List<EventRequestDTO> batch) {
        Map<String, EventRequestDTO> uniqueMap = new LinkedHashMap<>();
        int intraUpdates = 0;
        int intraDedups = 0;

        for (EventRequestDTO current : batch) {
            String id = current.getEventId();

            if (uniqueMap.containsKey(id)) {
                EventRequestDTO previous = uniqueMap.get(id);

                if (isSameData(previous, current)) {
                    intraDedups++;
                } else {
                    intraUpdates++;
                }
                uniqueMap.put(id, current);
            } else {
                uniqueMap.put(id, current);
            }
        }

        return new BatchPreProcessResult(new ArrayList<>(uniqueMap.values()), intraUpdates, intraDedups);
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

    private DbPersistResult persistBatch(List<EventRequestDTO> validEvents,int intraUpdates, int intraDedups) {
        if (validEvents.isEmpty()) {
            return new DbPersistResult(0, 0, 0);
        }

        try {
            List<Map<String, Object>> dbRows = new ArrayList<>();
            for (EventRequestDTO event : validEvents) {
                Map<String, Object> row = new HashMap<>();

                row.put("event_id", event.getEventId());
                row.put("machine_id", event.getMachineId());

                row.put("event_time", event.getEventTime());
                row.put("received_time", Instant.now());

                row.put("defect_count", event.getDefectCount());
                row.put("duration_ms", event.getDurationMs());

                dbRows.add(row);
            }

            String json = objectMapper.writeValueAsString(dbRows);

            List<Map<String, Object>> resultRows = repository.processBatchInDB(json);

            Map<String, Integer> counts = new HashMap<>();
            for (Map<String, Object> row : resultRows) {
                String status = (String) row.get("status");
                Number count = (Number) row.get("count");
                counts.put(status, count != null ? count.intValue() : 0);
            }

            int accepted = counts.getOrDefault(Constants.ACCEPTED.getCode(), 0);
            int updated = counts.getOrDefault(Constants.UPDATED.getCode(), 0);
            int deduped = counts.getOrDefault(Constants.DEDUPED.getCode(), 0);
            updated+= intraUpdates;
            deduped+= intraDedups;

            return new DbPersistResult(accepted, updated, deduped);

        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error serializing event batch for database persistence", e);
        } catch (Exception e) {
            throw new RuntimeException("Database error during event batch persistence", e);
        }
    }

    private boolean isSameData(EventRequestDTO oldEvt, EventRequestDTO newEvt) {
        return Objects.equals(oldEvt.getMachineId(), newEvt.getMachineId()) &&
                Objects.equals(oldEvt.getEventTime(), newEvt.getEventTime()) &&
                Objects.equals(oldEvt.getDurationMs(), newEvt.getDurationMs()) &&
                Objects.equals(oldEvt.getDefectCount(), newEvt.getDefectCount());
    }

    private Optional<String> runValidations(EventRequestDTO event) {
        return validationStrategies.stream()
                .map(s -> s.validate(event))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    private record BatchPreProcessResult(List<EventRequestDTO> sanitizedList, int intraUpdates, int intraDedups) {}

    private record ValidationResult(List<EventRequestDTO> validEvents, List<IngestResponseDTO.Rejection> rejections) {}

    private record DbPersistResult(int accepted, int updated, int dbDeduped) {}
}