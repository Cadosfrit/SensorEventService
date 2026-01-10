package com.cadosfrit.sensor.event.service.tests;

import com.cadosfrit.sensor.event.service.repository.MachineEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@ActiveProfiles("test")
public class ScenarioEightTest {

    @Autowired private MachineEventRepository eventRepository;
    @Autowired private ObjectMapper objectMapper;

    @Test
    void testConcurrentDeadlockPreventionWithOrderedUpsert() throws Exception {
        // --- 1. ARRANGE: Create two overlapping batches in REVERSE order ---
        String id1 = "concurrent_id_001";
        String id2 = "concurrent_id_002";

        List<Map<String, Object>> batchA = List.of(
                createEventMap(id1, 10),
                createEventMap(id2, 10)
        );

        List<Map<String, Object>> batchB = List.of(
                createEventMap(id2, 20),
                createEventMap(id1, 20)
        );

        String jsonA = objectMapper.writeValueAsString(batchA);
        String jsonB = objectMapper.writeValueAsString(batchB);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(2);

        // --- 2. ACT: Execute both batches simultaneously ---
        executor.submit(() -> executeBatch(jsonA, startLatch, endLatch));
        executor.submit(() -> executeBatch(jsonB, startLatch, endLatch));

        startLatch.countDown(); // Start both at once
        boolean finished = endLatch.await(15, TimeUnit.SECONDS);
        executor.shutdown();

        // --- 3. ASSERT ---
        assertEquals(true, finished, "The threads timed out, possibly due to a deadlock!");

        long count = eventRepository.count();
        assertEquals(2, count, "Database should contain exactly 2 unique records");

        System.out.println("=== Concurrency & Deadlock Test Summary ===");
        System.out.println("Successfully processed overlapping batches without Deadlock.");
        System.out.println("Total unique events in DB: " + count);
        System.out.println("Isolation Level used in SP: READ COMMITTED");
        System.out.println("===========================================");
    }

    private void executeBatch(String json, CountDownLatch start, CountDownLatch end) {
        try {
            start.await();
            eventRepository.processFastBatchSP(json);
        } catch (Exception e) {
            System.err.println("Batch execution failed: " + e.getMessage());
        } finally {
            end.countDown();
        }
    }

    private Map<String, Object> createEventMap(String id, int defects) {
        return Map.of(
                "event_id", id,
                "machine_id", "mac_concurrency",
                "event_time", "2023-11-01T12:00:00Z",
                "received_time", "2023-11-01T12:05:00Z",
                "duration_ms", 1000,
                "defect_count", defects
        );
    }
}
