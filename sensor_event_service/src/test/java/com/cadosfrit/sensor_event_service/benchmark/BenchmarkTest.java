package com.cadosfrit.sensor_event_service.benchmark;

import com.cadosfrit.sensor_event_service.dto.EventRequestDTO;
import com.cadosfrit.sensor_event_service.dto.IngestResponseDTO;
import com.cadosfrit.sensor_event_service.model.MachineEvent;
import com.cadosfrit.sensor_event_service.repository.MachineEventRepository;
import com.cadosfrit.sensor_event_service.service.EventIngestService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.ActiveProfiles;

import java.io.InputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

@Slf4j
@SpringBootTest
@ActiveProfiles("test")
public class BenchmarkTest {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private MachineEventRepository machineEventRepository;

    @Autowired
    @Qualifier("EventIngestServiceV1")
    private EventIngestService ingestServiceV1;

    @Autowired
    @Qualifier("EventIngestServiceV2")
    private EventIngestService ingestServiceV2;

    @BeforeAll
    public static void setupBenchmark() {
        log.info(generateSeparator());
        log.info("BENCHMARK TEST SETUP STARTED");
        log.info(generateSeparator());
    }

    @Test
    public void runBenchmarkTest() throws Exception {
        log.info("Starting Benchmark Test...");

        setupDatabaseWithMasterData();

        List<EventRequestDTO> benchmarkEvents = loadBenchmarkData();
        log.info("Loaded {} benchmark events", benchmarkEvents.size());

        long v1Duration = runBenchmarkV1(benchmarkEvents);

        setupDatabaseWithMasterData();

        long v2Duration = runBenchmarkV2(benchmarkEvents);

        displayBenchmarkResults(benchmarkEvents.size(), v1Duration, v2Duration);

        machineEventRepository.deleteAll();
        log.info("Benchmark Test completed and database cleaned up.");
    }

    /**
     * Setup database with master data from JSON files in resources/data
     * Actually inserts the data into the database
     */
    private void setupDatabaseWithMasterData() throws Exception {
        try {
            log.info("Setting up database with master data...");

            try {
                List<MachineEvent> machineEvents = loadMachineEventsFromJson("data/machine_events_prepopulate.json");
                if (!machineEvents.isEmpty()) {
                    machineEventRepository.deleteAll();
                    machineEventRepository.saveAll(machineEvents);
                    log.info("Successfully inserted {} pre-populated machine events into database", machineEvents.size());
                } else {
                    log.debug("No machine events to insert");
                }
            } catch (Exception e) {
                log.warn("Could not load or insert machine events from prepopulate file: {}", e.getMessage());
            }

            log.info("Database setup complete - all master data inserted");

        } catch (Exception e) {
            log.error("Error setting up database with master data", e);
            throw e;
        }
    }

    /**
     * Load MachineEvent objects from JSON file and convert them
     */
    private List<MachineEvent> loadMachineEventsFromJson(String filePath) throws Exception {
        try {
            ClassPathResource resource = new ClassPathResource(filePath);

            if (!resource.exists()) {
                log.warn("Machine events file not found: {}", filePath);
                return List.of();
            }

            try (InputStream inputStream = resource.getInputStream()) {
                MachineEvent[] events = objectMapper.readValue(inputStream, MachineEvent[].class);
                log.debug("Loaded {} machine events from JSON file", events.length);
                return Arrays.asList(events);
            }

        } catch (Exception e) {
            log.warn("Error loading machine events from JSON file: {}", filePath, e);
            return List.of();
        }
    }

    /**
     * Load benchmark data from JSON file
     */
    private List<EventRequestDTO> loadBenchmarkData() throws Exception {
        try {
            ClassPathResource resource = new ClassPathResource("data/benchmark_events_1000.json");

            if (!resource.exists()) {
                log.error("Benchmark data file not found at: data/benchmark_events_1000.json");
                throw new RuntimeException("Benchmark data file not found");
            }

            try (InputStream inputStream = resource.getInputStream()) {
                EventRequestDTO[] events = objectMapper.readValue(inputStream, EventRequestDTO[].class);

                Instant now = Instant.now();
                for (EventRequestDTO event : events) {
                    if (event.getReceivedTime() == null) {
                        event.setReceivedTime(now);
                    }
                }

                log.info("Successfully loaded {} benchmark events", events.length);
                return List.of(events);
            }

        } catch (Exception e) {
            log.error("Error loading benchmark data", e);
            throw e;
        }
    }

    /**
     * Run EventIngestServiceV1 benchmark
     */
    private long runBenchmarkV1(List<EventRequestDTO> benchmarkEvents) {
        try {
            log.info("");
            log.info(generateSeparator());
            log.info("RUNNING BENCHMARK: EventIngestServiceV1");
            log.info(generateSeparator());

            long startTime = System.currentTimeMillis();

            IngestResponseDTO response = ingestServiceV1.processBatch(benchmarkEvents);

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;

            log.info("V1 Benchmark Results:");
            log.info("  - Total Events: {}", benchmarkEvents.size());
            log.info("  - Accepted: {}", response.getAccepted());
            log.info("  - Updated: {}", response.getUpdated());
            log.info("  - Deduped: {}", response.getDeduped());
            log.info("  - Rejected: {}", response.getRejected());
            log.info("  - Time Taken: {} ms", duration);
            double v1Throughput = (benchmarkEvents.size() * 1000.0) / duration;
            log.info("  - Throughput: {} events/sec", String.format("%.2f", v1Throughput));

            return duration;

        } catch (Exception e) {
            log.error("Error running V1 benchmark", e);
            throw new RuntimeException("V1 Benchmark failed", e);
        }
    }

    /**
     * Run EventIngestServiceV2 benchmark
     */
    private long runBenchmarkV2(List<EventRequestDTO> benchmarkEvents) {
        try {
            log.info("");
            log.info(generateSeparator());
            log.info("RUNNING BENCHMARK: EventIngestServiceV2");
            log.info(generateSeparator());

            long startTime = System.currentTimeMillis();

            IngestResponseDTO response = ingestServiceV2.processBatch(benchmarkEvents);

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;

            log.info("V2 Benchmark Results:");
            log.info("  - Total Events: {}", benchmarkEvents.size());
            log.info("  - Accepted: {}", response.getAccepted());
            log.info("  - Updated: {}", response.getUpdated());
            log.info("  - Deduped: {}", response.getDeduped());
            log.info("  - Rejected: {}", response.getRejected());
            log.info("  - Time Taken: {} ms", duration);
            double v2Throughput = (benchmarkEvents.size() * 1000.0) / duration;
            log.info("  - Throughput: {} events/sec", String.format("%.2f", v2Throughput));

            return duration;

        } catch (Exception e) {
            log.error("Error running V2 benchmark", e);
            throw new RuntimeException("V2 Benchmark failed", e);
        }
    }

    /**
     * Display comprehensive benchmark comparison results
     */
    private void displayBenchmarkResults(int totalEvents, long v1Duration, long v2Duration) {
        log.info("");
        log.info(generateSeparator());
        log.info("BENCHMARK COMPARISON RESULTS");
        log.info(generateSeparator());
        log.info("");
        log.info("Total Events Ingested: {}", totalEvents);
        log.info("");
        log.info("EventIngestServiceV1:");
        log.info("  - Time: {} ms", v1Duration);
        double v1Throughput = (totalEvents * 1000.0) / v1Duration;
        log.info("  - Throughput: {} events/sec", String.format("%.2f", v1Throughput));
        log.info("");
        log.info("EventIngestServiceV2:");
        log.info("  - Time: {} ms", v2Duration);
        double v2Throughput = (totalEvents * 1000.0) / v2Duration;
        log.info("  - Throughput: {} events/sec", String.format("%.2f", v2Throughput));
        log.info("");

        long difference = Math.abs(v1Duration - v2Duration);
        double percentDifference = (difference * 100.0) / Math.max(v1Duration, v2Duration);

        if (v2Duration < v1Duration) {
            log.info("WINNER: EventIngestServiceV2");
            String percentStr = String.format("%.2f", percentDifference);
            log.info("  - Faster by: {} ms ({}%)", difference, percentStr);
        } else if (v1Duration < v2Duration) {
            log.info("WINNER: EventIngestServiceV1");
            String percentStr = String.format("%.2f", percentDifference);
            log.info("  - Faster by: {} ms ({}%)", difference, percentStr);
        } else {
            log.info("RESULT: Both services performed equally");
        }

        log.info("");
        log.info(generateSeparator());
        log.info("BENCHMARK TEST COMPLETED");
        log.info(generateSeparator());
        log.info("Note: All data will be rolled back after test completion (transactional)");
    }

    /**
     * Generate a separator line for log output
     */
    private static String generateSeparator() {
        return "=".repeat(80);
    }
}
