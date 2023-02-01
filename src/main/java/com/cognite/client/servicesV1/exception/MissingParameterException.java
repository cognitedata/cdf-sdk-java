package com.cognite.client.servicesV1.exception;

public class MissingParameterException extends RuntimeException {
    public MissingParameterException(String errorMessage) {
        super(errorMessage);
    }
}
