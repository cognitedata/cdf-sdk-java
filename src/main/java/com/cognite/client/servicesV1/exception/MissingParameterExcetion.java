package com.cognite.client.servicesV1.exception;

public class MissingParameterExcetion extends RuntimeException {
    public MissingParameterExcetion(String errorMessage) {
        super(errorMessage);
    }
}
