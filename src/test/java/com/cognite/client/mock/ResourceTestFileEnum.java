package com.cognite.client.mock;

public enum ResourceTestFileEnum {
    LOGIN_STATUS_LOGGED_ID_RESPONSE("json/loginBodyResponse.logged-in.json"),
    LOGIN_STATUS_NOT_LOGGED_ID_RESPONSE("json/loginBodyResponse.not-logged-in.json");

    private String value;

    ResourceTestFileEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
