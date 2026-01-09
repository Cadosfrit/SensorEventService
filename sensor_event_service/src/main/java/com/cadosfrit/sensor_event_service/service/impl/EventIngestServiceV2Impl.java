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
        try {
            if (batch == null || batch.isEmpty()) {
                log.warn("Received empty batch");
                return buildResponse(new DbPersistResult(0, 0, 0), new ValidationResult(Collections.emptyList(), Collections.emptyList()));
            }

            log.debug("Processing batch of {} events", batch.size());
            ValidationResult validationResult = validateEvents(batch);

            if (validationResult.validEvents.isEmpty()) {
                log.warn("All {} events rejected during validation", batch.size());
                return buildResponse(new DbPersistResult(0, 0, 0), validationResult);
            }

            SplitBatchResult splitResult = splitBatch(validationResult.validEvents);
            DbPersistResult fastResult = persistFastBatch(splitResult.fastBatch);
            DbPersistResult slowResult = persistSlowBatch(splitResult.slowBatch);
            DbPersistResult totalResult = mergeResults(fastResult, slowResult);

            log.info("Batch processed - Accepted: {}, Updated: {}, Deduped: {}, Rejected: {}",
                totalResult.accepted, totalResult.updated, totalResult.deduped, validationResult.rejections.size());

            return buildResponse(totalResult, validationResult);

        } catch (Exception e) {
            log.error("Error processing batch", e);
            return buildResponse(new DbPersistResult(0, 0, 0), new ValidationResult(Collections.emptyList(), Collections.emptyList()));
        }
    }

    private SplitBatchResult splitBatch(List<EventRequestDTO> events) {
        try {
            if (events == null || events.isEmpty()) {
                return new SplitBatchResult(Collections.emptyList(), Collections.emptyList());
            }

            Map<String, Integer> frequencyMap = new HashMap<>();
            for (EventRequestDTO event : events) {
                if (event != null && event.getEventId() != null) {
                    frequencyMap.merge(event.getEventId(), 1, Integer::sum);
                }
            }

            List<EventRequestDTO> fastBatch = new ArrayList<>();
            List<EventRequestDTO> slowBatch = new ArrayList<>();

            for (EventRequestDTO event : events) {
                if (event == null || event.getEventId() == null) {
                    log.warn("Skipping null event or null eventId");
                    continue;
                }

                if (frequencyMap.get(event.getEventId()) > 1) {
                    slowBatch.add(event);
                } else {
                    fastBatch.add(event);
                }
            }

            log.debug("Batch split - Fast: {}, Slow: {}", fastBatch.size(), slowBatch.size());
            return new SplitBatchResult(fastBatch, slowBatch);

        } catch (Exception e) {
            log.error("Error splitting batch", e);
            return new SplitBatchResult(Collections.emptyList(), Collections.emptyList());
        }
    }

    private DbPersistResult persistFastBatch(List<EventRequestDTO> events) {
        try {
            if (events == null || events.isEmpty()) {
                return new DbPersistResult(0, 0, 0);
            }

            String json = convertToDbJson(events);
            List<Map<String, Object>> rows = repository.processFastBatchSP(json);
            DbPersistResult result = parseDbStats(rows);
            log.debug("Fast batch persisted - Events: {}, Accepted: {}", events.size(), result.accepted);
            return result;

        } catch (JsonProcessingException e) {
            log.error("JSON processing error in fast batch", e);
            return new DbPersistResult(0, 0, 0);
        } catch (Exception e) {
            log.error("Error persisting fast batch", e);
            return new DbPersistResult(0, 0, 0);
        }
    }

    private DbPersistResult persistSlowBatch(List<EventRequestDTO> events) {
        try {
            if (events == null || events.isEmpty()) {
                return new DbPersistResult(0, 0, 0);
            }

            String json = convertToDbJson(events);
            List<Map<String, Object>> rows = repository.processSlowBatchSP(json);
            DbPersistResult result = parseDbStats(rows);
            log.debug("Slow batch persisted - Events: {}, Accepted: {}", events.size(), result.accepted);
            return result;

        } catch (JsonProcessingException e) {
            log.error("JSON processing error in slow batch", e);
            return new DbPersistResult(0, 0, 0);
        } catch (Exception e) {
            log.error("Error persisting slow batch", e);
            return new DbPersistResult(0, 0, 0);
        }
    }

    private String convertToDbJson(List<EventRequestDTO> events) throws JsonProcessingException {
        try {
            if (events == null || events.isEmpty()) {
                return "[]";
            }

            List<Map<String, Object>> dbRows = new ArrayList<>();
            Instant now = Instant.now();

            for (EventRequestDTO event : events) {
                try {
                    if (event == null) {
                        log.warn("Skipping null event during JSON conversion");
                        continue;
                    }

                    Map<String, Object> row = new HashMap<>();
                    row.put("event_id", event.getEventId());
                    row.put("machine_id", event.getMachineId());
                    row.put("event_time", event.getEventTime());
                    row.put("received_time", now);
                    row.put("defect_count", event.getDefectCount());
                    row.put("duration_ms", event.getDurationMs());
                    dbRows.add(row);
                } catch (Exception e) {
                    log.warn("Error converting event to JSON: {}", event != null ? event.getEventId() : "unknown", e);
                }
            }

            return objectMapper.writeValueAsString(dbRows);

        } catch (JsonProcessingException e) {
            log.error("JSON serialization error", e);
            throw e;
        } catch (Exception e) {
            log.error("Unexpected error in convertToDbJson", e);
            throw new RuntimeException("Error converting events to JSON", e);
        }
    }

    private DbPersistResult parseDbStats(List<Map<String, Object>> dbRows) {
        try {
            if (dbRows == null || dbRows.isEmpty()) {
                return new DbPersistResult(0, 0, 0);
            }

            int accepted = 0;
            int updated = 0;
            int deduped = 0;

            for (Map<String, Object> row : dbRows) {
                try {
                    if (row == null) {
                        log.warn("Skipping null row in DB stats");
                        continue;
                    }

                    String status = (String) row.get("status");
                    Number countNum = (Number) row.get("count");
                    int count = (countNum != null) ? countNum.intValue() : 0;

                    if (status == null) {
                        log.warn("Null status in DB stats row");
                        continue;
                    }

                    if (Constants.ACCEPTED.getCode().equals(status)) accepted += count;
                    else if (Constants.UPDATED.getCode().equals(status)) updated += count;
                    else if (Constants.DEDUPED.getCode().equals(status)) deduped += count;
                } catch (ClassCastException e) {
                    log.warn("Error parsing DB stats row", e);
                }
            }

            return new DbPersistResult(accepted, updated, deduped);

        } catch (Exception e) {
            log.error("Error parsing DB stats", e);
            return new DbPersistResult(0, 0, 0);
        }
    }

    private DbPersistResult mergeResults(DbPersistResult r1, DbPersistResult r2) {
        return new DbPersistResult(
                r1.accepted + r2.accepted,
                r1.updated + r2.updated,
                r1.deduped + r2.deduped
        );
    }

    private ValidationResult validateEvents(List<EventRequestDTO> batch) {
        try {
            if (batch == null || batch.isEmpty()) {
                return new ValidationResult(Collections.emptyList(), Collections.emptyList());
            }

            List<EventRequestDTO> validEvents = new ArrayList<>();
            List<IngestResponseDTO.Rejection> rejections = new ArrayList<>();

            for (EventRequestDTO event : batch) {
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

            return new ValidationResult(validEvents, rejections);

        } catch (Exception e) {
            log.error("Error in validateEvents", e);
            return new ValidationResult(Collections.emptyList(), Collections.emptyList());
        }
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