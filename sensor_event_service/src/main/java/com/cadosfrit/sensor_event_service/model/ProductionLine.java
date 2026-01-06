package com.cadosfrit.sensor_event_service.model;

import jakarta.persistence.*;
import lombok.Data;

@Data
@Entity
@Table(name = "production_lines")
public class ProductionLine {
    @Id
    private String id;

    private String name;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "factory_id")
    private Factory factory;
}
