package com.cognite.client.servicesV1.exception;

/**
 * Signals that a Cognite Data Fusion specific I/O exception of some sort has occurred.
 */
public class CdfIOException extends Exception {
    public CdfIOException(String errorMessage) {
        super(errorMessage);
    }
}
