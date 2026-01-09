package com.cadosfrit.sensor_event_service.generatedata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class BenchmarkGenerator {

    public static void main(String[] args) {
        try {
            generateBenchmarkData();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void generateBenchmarkData() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);

        Random random = new Random();
        List<String> machines = new ArrayList<>();
        // Setup Hierarchy: 5 Factories, 2 Lines each, 3 Machines each
        for (int f = 1; f <= 5; f++) {
            for (int l = 1; l <= 2; l++) {
                for (int m = 1; m <= 3; m++) {
                    machines.add(String.format("mac_%d_L%d_F%d", m, l, f));
                }
            }
        }

        Instant startTime = Instant.parse("2023-11-01T12:00:00Z");
        int[] defectOptions = {-1, 0, 1, 2};
        List<ObjectNode> allEvents = new ArrayList<>();

        // 1. 80% Unique base events (800)
        List<ObjectNode> uniqueBase = new ArrayList<>();
        for (int i = 1; i <= 800; i++) {
            ObjectNode event = mapper.createObjectNode();
            event.put("eventId", String.format("evt_b_%04d", i));
            event.put("machineId", machines.get(random.nextInt(machines.size())));
            event.put("eventTime", startTime.plus(i, ChronoUnit.SECONDS).toString());
            event.put("durationMs", 500 + random.nextInt(4500));
            event.put("defectCount", defectOptions[random.nextInt(defectOptions.length)]);
            uniqueBase.add(event);
        }
        allEvents.addAll(uniqueBase);

        // 2. 5% Update (50) - Pick from uniqueBase and modify
        Collections.shuffle(uniqueBase);
        for (int i = 0; i < 50; i++) {
            ObjectNode update = uniqueBase.get(i).deepCopy();
            update.put("defectCount", defectOptions[random.nextInt(defectOptions.length)]);
            update.put("durationMs", update.get("durationMs").asInt() + 100);
            allEvents.add(update);
        }

        // 3. 5% Intra-batch update (50 events = 25 pairs)
        for (int i = 801; i <= 825; i++) {
            String id = String.format("evt_b_%04d", i);
            String mac = machines.get(random.nextInt(machines.size()));
            String ts = startTime.plus(i + 1000, ChronoUnit.SECONDS).toString();

            allEvents.add(createEvent(mapper, id, mac, ts, 1000, 0));
            allEvents.add(createEvent(mapper, id, mac, ts, 2000, 1));
        }

        // 4. 5% Duplicate (50) - Exact copies of base
        for (int i = 50; i < 100; i++) {
            allEvents.add(uniqueBase.get(i).deepCopy());
        }

        // 5. 5% Intra-batch duplicate (50 events = 25 pairs)
        for (int i = 826; i <= 850; i++) {
            String id = String.format("evt_b_%04d", i);
            String mac = machines.get(random.nextInt(machines.size()));
            String ts = startTime.plus(i + 2000, ChronoUnit.SECONDS).toString();
            ObjectNode event = createEvent(mapper, id, mac, ts, 3000, 2);
            allEvents.add(event);
            allEvents.add(event.deepCopy());
        }

        // Shuffle all to simulate real ingestion
        Collections.shuffle(allEvents);

        // Convert to ArrayNode and write to file
        ArrayNode rootArray = mapper.createArrayNode();
        rootArray.addAll(allEvents);

        File outputFile = new File("benchmark_events_1000.json");
        mapper.writeValue(outputFile, rootArray);

        System.out.println("Generated " + allEvents.size() + " events in: " + outputFile.getAbsolutePath());
    }

    private static ObjectNode createEvent(ObjectMapper mapper, String id, String mac, String ts, int dur, int def) {
        ObjectNode event = mapper.createObjectNode();
        event.put("eventId", id);
        event.put("machineId", mac);
        event.put("eventTime", ts);
        event.put("durationMs", dur);
        event.put("defectCount", def);
        return event;
    }
}