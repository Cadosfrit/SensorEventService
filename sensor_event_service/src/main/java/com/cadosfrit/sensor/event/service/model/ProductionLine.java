package com.cadosfrit.sensor.event.service.model;

import jakarta.persistence.*;
import lombok.Data;

@Data
@Entity
@Table(name = "production_lines")
public class ProductionLine {
    @Id
    @Column(name = "line_id", length = 50)
    private String lineId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "factory_id")
    private Factory factory;
}