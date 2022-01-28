package com.cognite.client.mock;

import com.cognite.client.TestConfigProvider;
import com.cognite.client.util.TestResponseBodyUtils;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;

public class MockResponseBodyGenerator {
    static final Request req = new Request.Builder()
                                        .url(TestConfigProvider.getHost())
                                        .build();

    public static Response loginStatusLoggedInResponse() throws IOException {
        return new Response.Builder()
                .body(
                    TestResponseBodyUtils.create(
                        ResourceTestFileEnum.LOGIN_STATUS_LOGGED_ID_RESPONSE.getValue(),
                        MediaTypeEnum.JSON_MEDIA_TYPE.getValue()
                    )
                )
                .code(200)
                .request(req)
                .protocol(Protocol.HTTP_2)
                .message("")
                .build();
    }

    public static Response loginStatusNotLoggedInResponse() throws IOException {
        return new Response.Builder()
                .body(
                    TestResponseBodyUtils.create(
                        ResourceTestFileEnum.LOGIN_STATUS_NOT_LOGGED_ID_RESPONSE.getValue(),
                        MediaTypeEnum.JSON_MEDIA_TYPE.getValue()
                    )
                )
                .code(200)
                .request(req)
                .protocol(Protocol.HTTP_2)
                .message("")
                .build();
    }
}
