package com.cadosfrit.sensor.event.service.tests;

import com.cadosfrit.sensor.event.service.constants.Constants;
import com.cadosfrit.sensor.event.service.dto.EventRequestDTO;
import com.cadosfrit.sensor.event.service.dto.IngestResponseDTO;
import com.cadosfrit.sensor.event.service.repository.MachineEventRepository;
import com.cadosfrit.sensor.event.service.service.EventIngestService;

import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@ActiveProfiles("test")
public class ScenarioFourTest {

    @Autowired
    @Qualifier("EventIngestServiceV1") EventIngestService eventIngestService;

    @Mock
    MachineEventRepository eventRepository;

    @Test
    void testFutureEventIsRejectedAndNotStoredInDB() throws Exception {

        // --- 2. ARRANGE: Create an invalid event (30 minutes in the future) ---
        String eventId = "evt_invalid_future";
        Instant futureTime = Instant.now().plus(30, ChronoUnit.MINUTES);
        EventRequestDTO invalidEvent = new EventRequestDTO();
        invalidEvent.setEventId(eventId);
        invalidEvent.setMachineId("mac_1");
        invalidEvent.setEventTime(futureTime);
        invalidEvent.setReceivedTime(Instant.now());
        invalidEvent.setDurationMs(5000L);
        invalidEvent.setDefectCount(0);

        // --- 3. ACT: Call the processing logic ---
        IngestResponseDTO responseDTO = eventIngestService.processBatch(List.of(invalidEvent));
        // --- 4. ASSERT: Verify the event was rejected ---
        assertEquals(1, responseDTO.getRejected(), "One event should be rejected");
        assertEquals(eventId,responseDTO.getRejections().getFirst().getEventId());
        assertEquals(Constants.FUTURE_EVENT_TIME.getCode(),responseDTO.getRejections().getFirst().getReason());

        Mockito.verify(eventRepository,Mockito.never()).processBatchInDB(Mockito.any());
    }
}