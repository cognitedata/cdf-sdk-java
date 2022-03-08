package com.cognite.client.util;

import okhttp3.MediaType;
import okhttp3.ResponseBody;

import java.io.IOException;

public class TestResponseBodyUtils {
    public static ResponseBody create(String jsonResourceFilePath, String mediaType) throws IOException {
        return ResponseBody.create(ResourceFileLoaderUtils.loadAsString(jsonResourceFilePath), MediaType.get(mediaType));
    }
}
