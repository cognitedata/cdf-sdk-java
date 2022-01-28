package com.cognite.client.mock;

public enum MediaTypeEnum {
    JSON_MEDIA_TYPE("application/json");

    private String value;

    MediaTypeEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
