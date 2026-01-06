package com.cadosfrit.sensor_event_service.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Data
@Entity
@Table(name = "factories")
public class Factory {
    @Id
    private String id;
    private String name;
}
    