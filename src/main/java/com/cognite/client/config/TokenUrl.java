package com.cognite.client.config;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Helper class that can generate token URLs from well known identity providers.
 */
public class TokenUrl {

    /**
     * Generate a token URL for authenticating towards Azure AD as the identity provider. You
     * have to specify the Azure AD tenant id as input.
     *
     * @param tenantId The Azure AD tenant id.
     * @return The token URL for your Azure AD tenant.
     * @throws MalformedURLException
     */
    public static URL generateAzureAdURL(String tenantId) throws MalformedURLException {
        return new URL(String.format("https://login.microsoftonline.com/%s/oauth2/v2.0/token",
                tenantId));
    }
}
