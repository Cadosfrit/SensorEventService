package com.cadosfrit.sensor.event.service.constants;


public final class AppConstants {
    // Prevent instantiation
    private AppConstants() {}

    // Validation Limits
    public static final long MAX_DURATION_MS = 6 * 60 * 60 * 1000;
    public static final int FUTURE_TIME_ALLOWANCE_MINS = 15;
}