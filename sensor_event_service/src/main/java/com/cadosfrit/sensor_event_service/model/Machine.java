package com.cadosfrit.sensor_event_service.model;

import jakarta.persistence.*;
import lombok.Data;

@Data
@Entity
@Table(name = "machines")
public class Machine {
    @Id
    private String machineId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "line_id")
    private ProductionLine productionLine;
}