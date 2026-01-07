package com.cadosfrit.sensor_event_service.stratergy;

import com.cadosfrit.sensor_event_service.dto.EventRequestDTO;
import java.util.Optional;

public interface EventValidationStrategy {
    /**
     * @return Optional containing the error reason if invalid, 
     * or Optional.empty() if valid.
     */
    Optional<String> validate(EventRequestDTO event);
}