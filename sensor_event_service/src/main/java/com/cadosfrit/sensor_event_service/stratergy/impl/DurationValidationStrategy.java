package com.cadosfrit.sensor_event_service.stratergy.impl;

import com.cadosfrit.sensor_event_service.constants.AppConstants;
import com.cadosfrit.sensor_event_service.constants.Constants;
import com.cadosfrit.sensor_event_service.dto.EventRequestDTO;
import com.cadosfrit.sensor_event_service.stratergy.EventValidationStrategy;

import org.springframework.stereotype.Component;
import java.util.Optional;

@Component
public class DurationValidationStrategy implements EventValidationStrategy {

    @Override
    public Optional<String> validate(EventRequestDTO event) {
        Long duration = event.getDurationMs();
        if (duration == null || duration < 0 || duration > AppConstants.MAX_DURATION_MS) {
            return Optional.of(Constants.INVALID_DURATION.getCode());
        }
        return Optional.empty();
    }
}