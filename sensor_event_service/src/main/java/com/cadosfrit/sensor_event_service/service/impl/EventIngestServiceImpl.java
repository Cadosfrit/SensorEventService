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

@Service("EventIngestServiceV1")
@RequiredArgsConstructor
@Slf4j
public class EventIngestServiceImpl implements EventIngestService {

    private final ObjectMapper objectMapper;
    private final MachineEventRepository repository;
    private final List<EventValidationStrategy> validationStrategies;

    @Override
    @Transactional
    public IngestResponseDTO processBatch(List<EventRequestDTO> batch) {
        try {
            if (batch == null || batch.isEmpty()) {
                log.warn("Received empty or null batch");
                return buildEmptyResponse();
            }

            log.debug("Processing batch of {} events", batch.size());
            BatchPreProcessResult preProcessResult = preprocessBatch(batch);

            ValidationResult validationResult = validateEvents(preProcessResult.sanitizedList);

            DbPersistResult dbResult = persistBatch(validationResult.validEvents,
                    preProcessResult.intraUpdates,
                    preProcessResult.intraDedups);

            log.info("Batch processed - Accepted: {}, Updated: {}, Deduped: {}, Rejected: {}",
                dbResult.accepted, dbResult.updated, dbResult.dbDeduped, validationResult.rejections.size());

            return IngestResponseDTO.builder()
                    .accepted(dbResult.accepted)
                    .updated(dbResult.updated)
                    .deduped(dbResult.dbDeduped)
                    .rejected(validationResult.rejections.size())
                    .rejections(validationResult.rejections)
                    .build();
        } catch (Exception e) {
            log.error("Error processing batch", e);
            return buildEmptyResponse();
        }
    }

    private BatchPreProcessResult preprocessBatch(List<EventRequestDTO> batch) {
        try {
            if (batch == null || batch.isEmpty()) {
                return new BatchPreProcessResult(Collections.emptyList(), 0, 0);
            }

            Map<String, EventRequestDTO> uniqueMap = new LinkedHashMap<>();
            int intraUpdates = 0;
            int intraDedups = 0;

            for (EventRequestDTO current : batch) {
                try {
                    if (current == null || current.getEventId() == null) {
                        log.warn("Skipping null event or null eventId");
                        continue;
                    }

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
                } catch (Exception e) {
                    log.warn("Error preprocessing event", e);
                }
            }

            log.debug("Batch preprocessed - IntraUpdates: {}, IntraDedups: {}", intraUpdates, intraDedups);
            return new BatchPreProcessResult(new ArrayList<>(uniqueMap.values()), intraUpdates, intraDedups);
        } catch (Exception e) {
            log.error("Error in preprocessBatch", e);
            return new BatchPreProcessResult(Collections.emptyList(), 0, 0);
        }
    }

    private ValidationResult validateEvents(List<EventRequestDTO> uniqueEvents) {
        try {
            if (uniqueEvents == null || uniqueEvents.isEmpty()) {
                return new ValidationResult(Collections.emptyList(), Collections.emptyList());
            }

            List<EventRequestDTO> validEvents = new ArrayList<>();
            List<IngestResponseDTO.Rejection> rejections = new ArrayList<>();

            for (EventRequestDTO event : uniqueEvents) {
                try {
                    if (event == null) {
                        log.warn("Skipping null event during validation");
                        continue;
                    }

                    Optional<String> error = runValidations(event);
                    if (error.isPresent()) {
                        rejections.add(new IngestResponseDTO.Rejection(event.getEventId(), error.get()));
                    } else {
                        validEvents.add(event);
                    }
                } catch (Exception e) {
                    log.warn("Error validating event: {}", event != null ? event.getEventId() : "unknown", e);
                }
            }

            log.debug("Validation complete - Valid: {}, Rejected: {}", validEvents.size(), rejections.size());
            return new ValidationResult(validEvents, rejections);
        } catch (Exception e) {
            log.error("Error in validateEvents", e);
            return new ValidationResult(Collections.emptyList(), Collections.emptyList());
        }
    }

    private DbPersistResult persistBatch(List<EventRequestDTO> validEvents, int intraUpdates, int intraDedups) {
        try {
            if (validEvents == null || validEvents.isEmpty()) {
                return new DbPersistResult(0, 0, 0);
            }

            List<Map<String, Object>> dbRows = new ArrayList<>();
            for (EventRequestDTO event : validEvents) {
                try {
                    if (event == null) {
                        log.warn("Skipping null event during persistence");
                        continue;
                    }

                    Map<String, Object> row = new HashMap<>();
                    row.put("event_id", event.getEventId());
                    row.put("machine_id", event.getMachineId());
                    row.put("event_time", event.getEventTime());
                    row.put("received_time", Instant.now());
                    row.put("defect_count", event.getDefectCount());
                    row.put("duration_ms", event.getDurationMs());
                    dbRows.add(row);
                } catch (Exception e) {
                    log.warn("Error building database row for event", e);
                }
            }

            String json = objectMapper.writeValueAsString(dbRows);
            List<Map<String, Object>> resultRows = repository.processBatchInDB(json);

            Map<String, Integer> counts = new HashMap<>();
            for (Map<String, Object> row : resultRows) {
                try {
                    if (row == null) {
                        log.warn("Null row in database result");
                        continue;
                    }

                    String status = (String) row.get("status");
                    Number count = (Number) row.get("count");
                    counts.put(status, count != null ? count.intValue() : 0);
                } catch (ClassCastException e) {
                    log.warn("Error parsing database result row", e);
                }
            }

            int accepted = counts.getOrDefault(Constants.ACCEPTED.getCode(), 0);
            int updated = counts.getOrDefault(Constants.UPDATED.getCode(), 0);
            int deduped = counts.getOrDefault(Constants.DEDUPED.getCode(), 0);
            updated += intraUpdates;
            deduped += intraDedups;

            log.debug("Database persist complete - Accepted: {}, Updated: {}, Deduped: {}", accepted, updated, deduped);
            return new DbPersistResult(accepted, updated, deduped);

        } catch (JsonProcessingException e) {
            log.error("JSON serialization error during batch persistence", e);
            return new DbPersistResult(0, 0, 0);
        } catch (Exception e) {
            log.error("Error persisting batch to database", e);
            return new DbPersistResult(0, 0, 0);
        }
    }

    private boolean isSameData(EventRequestDTO oldEvt, EventRequestDTO newEvt) {
        return Objects.equals(oldEvt.getMachineId(), newEvt.getMachineId()) &&
                Objects.equals(oldEvt.getEventTime(), newEvt.getEventTime()) &&
                Objects.equals(oldEvt.getDurationMs(), newEvt.getDurationMs()) &&
                Objects.equals(oldEvt.getDefectCount(), newEvt.getDefectCount());
    }

    private IngestResponseDTO buildEmptyResponse() {
        return IngestResponseDTO.builder()
                .accepted(0)
                .updated(0)
                .deduped(0)
                .rejected(0)
                .rejections(Collections.emptyList())
                .build();
    }

    private Optional<String> runValidations(EventRequestDTO event) {
        try {
            if (event == null || validationStrategies == null || validationStrategies.isEmpty()) {
                return Optional.empty();
            }

            for (EventValidationStrategy strategy : validationStrategies) {
                try {
                    Optional<String> error = strategy.validate(event);
                    if (error.isPresent()) {
                        return error;
                    }
                } catch (Exception e) {
                    log.warn("Error running validation strategy", e);
                }
            }
            return Optional.empty();
        } catch (Exception e) {
            log.error("Error in runValidations", e);
            return Optional.empty();
        }
    }

    private record BatchPreProcessResult(List<EventRequestDTO> sanitizedList, int intraUpdates, int intraDedups) {}

    private record ValidationResult(List<EventRequestDTO> validEvents, List<IngestResponseDTO.Rejection> rejections) {}

    private record DbPersistResult(int accepted, int updated, int dbDeduped) {}
}