package com.cognite.client.servicesV1.exception;

import java.io.IOException;

/**
 * Indicates an invalid response from Cognite Data Fusion.
 *
 * When making a request to Cognite Data Fusion, the system may respond with various response codes depending on
 * how the request was processed. If the response code is not within the defined range of valid codes, this exception
 * is thrown.
 */
public class InvalidResponseException extends IOException {
    public InvalidResponseException(String errorMessage) {
        super(errorMessage);
    }
}
