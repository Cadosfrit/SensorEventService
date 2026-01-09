package com.cadosfrit.sensor_event_service.model;

import jakarta.persistence.*;
import lombok.Data;
import java.util.List;

@Data
@Entity
@Table(name = "factories")
public class Factory {

    @Id
    @Column(name = "factory_id", length = 50)
    private String factoryId;

    @OneToMany(mappedBy = "factory", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private List<ProductionLine> lines;
}