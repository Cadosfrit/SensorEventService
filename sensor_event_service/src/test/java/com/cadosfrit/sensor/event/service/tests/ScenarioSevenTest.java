package com.cadosfrit.sensor.event.service.tests;

import com.cadosfrit.sensor.event.service.dto.MachineStatsDTO;
import com.cadosfrit.sensor.event.service.model.*;
import com.cadosfrit.sensor.event.service.repository.*;
import com.cadosfrit.sensor.event.service.service.MachineStatsService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@ActiveProfiles("test")
public class ScenarioSevenTest {

    @Autowired private FactoryRepository factoryRepository;
    @Autowired private ProductionLineRepository lineRepository;
    @Autowired private MachineRepository machineRepository;
    @Autowired private MachineEventRepository eventRepository;
    @Autowired private MachineStatsService machineStatsService;

    @BeforeEach
    @AfterEach
    void cleanAndSetup() {
        eventRepository.deleteAll();
        machineRepository.deleteAll();
        lineRepository.deleteAll();
        factoryRepository.deleteAll();
    }

    @Test
    void testBoundaryCorrectnessEndIsExclusive() throws Exception {
        // --- 1. ARRANGE: Setup Master Data ---
        Factory factory=new Factory();
        factory.setFactoryId("F1");
        factoryRepository.saveAndFlush(factory);
        ProductionLine line = new ProductionLine();
        line.setLineId("L1");
        line.setFactory(factory);
        lineRepository.saveAndFlush(line);

        Machine machine= new Machine();
        machine.setMachineId("mac_1");
        machine.setProductionLine(line);
        machineRepository.saveAndFlush(machine);

        Instant windowStart = Instant.parse("2023-11-01T10:00:00Z");
        Instant windowEnd = Instant.parse("2023-11-01T11:00:00Z");

        // --- 2. ARRANGE: Create 3 events ---
        MachineEvent e1 = createEvent("evt_1", windowStart.plusSeconds(600));
        MachineEvent e2 = createEvent("evt_2", windowStart);
        MachineEvent e3 = createEvent("evt_3", windowEnd);

        eventRepository.saveAllAndFlush(List.of(e1, e2, e3));

        // --- 3. ACT: Call stats for the specific window ---
        MachineStatsDTO stats = machineStatsService.getMachineStats("mac_1", windowStart, windowEnd);

        // --- 4. ASSERT ---
        assertEquals(2, stats.getEventsCount(), "Events at the exact 'end' timestamp should be excluded");
        assertEquals(2, stats.getDefectsCount(), "Total defects should only sum events within [start, end)");

        System.out.println("=== Test Summary (Boundary) ===");
        System.out.println("Window: " + windowStart + " to " + windowEnd);
        System.out.println("Events found: " + stats.getEventsCount());
        System.out.println("Defects found: " + stats.getDefectsCount());
        System.out.println("===============================");
    }

    private MachineEvent createEvent(String id, Instant time) {
        MachineEvent event = new MachineEvent();
        event.setEventId(id);
        event.setMachineId("mac_1");
        event.setDefectCount(1);
        event.setDurationMs(1000L);
        event.setEventTime(time);
        event.setReceivedTime(Instant.now());
        return event;
    }
}