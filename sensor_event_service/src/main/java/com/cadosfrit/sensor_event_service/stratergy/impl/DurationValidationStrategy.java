package com.cadosfrit.sensor_event_service.stratergy.impl;

import com.cadosfrit.sensor_event_service.constants.AppConstants;
import com.cadosfrit.sensor_event_service.constants.Constants;
import com.cadosfrit.sensor_event_service.dto.EventRequestDTO;
import com.cadosfrit.sensor_event_service.stratergy.EventValidationStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import java.util.Optional;

@Component
public class DurationValidationStrategy implements EventValidationStrategy {

    private static final Logger logger = LoggerFactory.getLogger(DurationValidationStrategy.class);

    @Override
    public Optional<String> validate(EventRequestDTO event) {
        try {
            if (event == null) {
                logger.warn("DurationValidationStrategy: Event object is null");
                return Optional.of(Constants.INVALID_DURATION.getCode());
            }

            Long duration = event.getDurationMs();

            if (duration == null) {
                logger.warn("DurationValidationStrategy: Duration is null for eventId: {}",
                    event.getEventId());
                return Optional.of(Constants.INVALID_DURATION.getCode());
            }

            if (duration < 0) {
                logger.warn("DurationValidationStrategy: Negative duration {} for eventId: {}",
                    duration, event.getEventId());
                return Optional.of(Constants.INVALID_DURATION.getCode());
            }

            if (duration > AppConstants.MAX_DURATION_MS) {
                logger.warn("DurationValidationStrategy: Duration {} exceeds maximum {} for eventId: {}",
                    duration, AppConstants.MAX_DURATION_MS, event.getEventId());
                return Optional.of(Constants.INVALID_DURATION.getCode());
            }

            logger.debug("DurationValidationStrategy: Duration {} is valid for eventId: {}",
                duration, event.getEventId());
            return Optional.empty();

        } catch (NullPointerException e) {
            logger.error("DurationValidationStrategy: NullPointerException during validation", e);
            return Optional.of(Constants.INVALID_DURATION.getCode());
        } catch (ClassCastException e) {
            logger.error("DurationValidationStrategy: ClassCastException during duration validation", e);
            return Optional.of(Constants.INVALID_DURATION.getCode());
        } catch (Exception e) {
            logger.error("DurationValidationStrategy: Unexpected error during validation", e);
            return Optional.of(Constants.INVALID_DURATION.getCode());
        }
    }
}