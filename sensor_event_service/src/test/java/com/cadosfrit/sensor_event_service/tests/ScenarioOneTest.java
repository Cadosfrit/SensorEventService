package com.cadosfrit.sensor_event_service.tests;

import com.cadosfrit.sensor_event_service.model.Factory;
import com.cadosfrit.sensor_event_service.model.Machine;
import com.cadosfrit.sensor_event_service.model.MachineEvent;
import com.cadosfrit.sensor_event_service.model.ProductionLine;
import com.cadosfrit.sensor_event_service.repository.FactoryRepository;
import com.cadosfrit.sensor_event_service.repository.MachineEventRepository;
import com.cadosfrit.sensor_event_service.repository.MachineRepository;
import com.cadosfrit.sensor_event_service.repository.ProductionLineRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@ActiveProfiles("test")
public class ScenarioOneTest {

    @Autowired
    private FactoryRepository factoryRepository;

    @Autowired
    private ProductionLineRepository lineRepository;

    @Autowired
    private MachineRepository machineRepository;

    @Autowired
    private MachineEventRepository eventRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    @AfterEach
    void cleanAndSetup() {
        eventRepository.deleteAll();
        machineRepository.deleteAll();
        lineRepository.deleteAll();
        factoryRepository.deleteAll();
    }

    @Test
    void testIdenticalDuplicateEventIsDeduped() throws Exception {
        // --- 1. ARRANGE: Setup Master Data ---
        Factory factory = new Factory();
        factory.setFactoryId("F1");
        factoryRepository.saveAndFlush(factory);

        ProductionLine line = new ProductionLine();
        line.setLineId("L1");
        line.setFactory(factory);
        lineRepository.saveAndFlush(line);

        Machine machine = new Machine();
        machine.setMachineId("mac_1");
        machine.setProductionLine(line);
        machineRepository.saveAndFlush(machine);

        // --- 2. ARRANGE: Pre-insert existing event ---
        String eventId = "evt_duplicate_test";
        MachineEvent existing = new MachineEvent();
        existing.setEventId(eventId);
        existing.setMachineId("mac_1");
        existing.setDurationMs(5000L);
        existing.setDefectCount(2);
        existing.setEventTime(Instant.parse("2023-11-01T12:00:00Z"));
        existing.setReceivedTime(Instant.now());

        eventRepository.saveAndFlush(existing);

        // --- 3. ACT: Prepare JSON with EXACT SAME data ---
        List<Map<String, Object>> batch = List.of(
                Map.of(
                        "event_id", eventId,
                        "machine_id", "mac_1",
                        "event_time", "2023-11-01T12:00:00Z",
                        "received_time", "2023-11-01T12:05:00Z",
                        "duration_ms", 5000,
                        "defect_count", 2
                )
        );
        String jsonPayload = objectMapper.writeValueAsString(batch);

        List<Map<String, Object>> results = null;
        MachineEvent resultEvent = null;

        try {
            results = eventRepository.processFastBatchSP(jsonPayload);

            // --- 4. ASSERT : Verify SP result indicates DEDUPED ---
            Map<String, Object> dedupedResult = results.stream()
                    .filter(r -> "DEDUPED".equals(r.get("status")))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Expected DEDUPED status not found in SP result"));

            assertEquals(1L, ((Number) dedupedResult.get("count")).longValue(),
                    "SP result should count 1 deduped record");

            assertEquals(1, eventRepository.count(),
                    "Total events in DB should still be 1 after sending duplicate");

            resultEvent = eventRepository.findById(eventId).orElseThrow();
            assertEquals(2, resultEvent.getDefectCount(), "Defect count should remain 2");
        } finally {
            long updatedCount = 0;
            long dedupedCount = 0;
            long acceptedCount = 0;
            if (results != null) {
                updatedCount = results.stream()
                        .filter(r -> "UPDATED".equals(r.get("status")))
                        .mapToLong(r -> ((Number) r.get("count")).longValue())
                        .sum();
                dedupedCount = results.stream()
                        .filter(r -> "DEDUPED".equals(r.get("status")))
                        .mapToLong(r -> ((Number) r.get("count")).longValue())
                        .sum();
                acceptedCount = results.stream()
                        .filter(r -> "ACCEPTED".equals(r.get("status")))
                        .mapToLong(r -> ((Number) r.get("count")).longValue())
                        .sum();
            }

            System.out.println("=== Test Summary ===");
            System.out.println("SP updated count: " + updatedCount);
            System.out.println("SP deduped count: " + dedupedCount);
            System.out.println("SP accepted count: " + acceptedCount);
            System.out.println("Total events in DB: " + eventRepository.count());
            if (resultEvent != null) {
                System.out.println("Event in DB: id=" + resultEvent.getEventId()
                        + ", machineId=" + resultEvent.getMachineId()
                        + ", durationMs=" + resultEvent.getDurationMs()
                        + ", defectCount=" + resultEvent.getDefectCount()
                        + ", eventTime=" + resultEvent.getEventTime()
                        + ", receivedTime=" + resultEvent.getReceivedTime());
            } else {
                System.out.println("Event details not available");
            }
            System.out.println("====================");
        }
    }
}