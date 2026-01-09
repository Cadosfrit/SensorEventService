package com.cadosfrit.sensor_event_service.model;

import jakarta.persistence.*;
import lombok.Data;

import java.time.Instant;

@Data
@Entity
@Table(name = "machine_events", indexes = {
    @Index(name = "idx_event_time", columnList = "event_time"),
    @Index(name = "idx_machine_id_time", columnList = "machine_id, event_time")
})
public class MachineEvent {

    @Id
    @Column(name = "event_id")
    private String eventId; 

    @JoinColumn(name="machine_id", nullable = false)
    private String machineId;

    @Column(name = "event_time", nullable = false)
    private Instant eventTime;

    @Column(name = "received_time", nullable = false)
    private Instant receivedTime;

    @Column(name = "duration_ms", nullable = false)
    private Long durationMs;

    @Column(name = "defect_count", nullable = false)
    private Integer defectCount;
    
}