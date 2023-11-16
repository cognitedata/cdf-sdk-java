package com.cognite.client.servicesV1.exception;

/**
 * Signals that CDF RAW did not find row with a specific key.
 */
public class CdfRawNotFoundException extends Exception {
    public CdfRawNotFoundException(String errorMessage) {
        super(errorMessage);
    }
}

