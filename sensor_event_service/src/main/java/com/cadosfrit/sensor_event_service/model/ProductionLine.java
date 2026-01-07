package com.cadosfrit.sensor_event_service.model;

import jakarta.persistence.*;
import lombok.Data;

@Data
@Entity
@Table(name = "production_lines")
public class ProductionLine {
    @Id
    private String id;

    @Column(name = "factory_id", nullable = false)
    private String factoryId;
}
