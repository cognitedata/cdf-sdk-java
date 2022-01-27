package com.cognite.client.util;

import com.nimbusds.jose.util.IOUtils;

import java.io.IOException;
import java.io.InputStream;

public class ResourceFileLoaderUtils {
    public static String loadAsString(String resourcePath) throws IOException {
        return IOUtils.readInputStreamToString(getResourceAsStream(resourcePath));
    }

    public static InputStream getResourceAsStream(String resourcePath) {
        return ResourceFileLoaderUtils.class.getClassLoader().getResourceAsStream(resourcePath);
    }
}
