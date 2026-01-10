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
import java.time.temporal.ChronoUnit;


import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@ActiveProfiles("test")
public class ScenarioSixTest {

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
    void testNegativeDefectCountIsIgnoredInStats() throws Exception {
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

        // --- 2. ARRANGE: Insert an event with -1 defects ---
        String eventId = "evt_negative_defect";
        Instant now = Instant.now();

        MachineEvent event = new MachineEvent();
        event.setEventId(eventId);
        event.setMachineId("mac_1");
        event.setDurationMs(1000L);
        event.setDefectCount(-1); // The "Invalid" value
        event.setEventTime(now.minus(5, ChronoUnit.MINUTES));
        event.setReceivedTime(now);
        eventRepository.saveAndFlush(event);

        // --- 3. ACT: Call MachineStatsService ---
        Instant start = now.minus(1, ChronoUnit.HOURS);
        Instant end = now.plus(1, ChronoUnit.HOURS);

        MachineStatsDTO stats = machineStatsService.getMachineStats("mac_1", start, end);

        // --- 4. ASSERT ---
        assertEquals(1, stats.getEventsCount(), "The event should be counted");
        assertEquals(0, stats.getDefectsCount(), "The -1 defect should be ignored or treated as 0");
        assertEquals(0.0, stats.getAvgDefectRate(), "Rate should be 0 if defects are ignored");

        System.out.println("=== Test Summary (Negative Defects) ===");
        System.out.println("Machine ID: " + stats.getMachineId());
        System.out.println("Events Counted: " + stats.getEventsCount());
        System.out.println("Defects Counted: " + stats.getDefectsCount());
        System.out.println("Calculated Rate: " + stats.getAvgDefectRate());
        System.out.println("=======================================");
    }
}
