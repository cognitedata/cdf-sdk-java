package com.cognite.client;

import com.cognite.client.config.ClientConfig;
import com.cognite.client.dto.LoginStatus;
import com.cognite.client.mock.MockResponseBodyGenerator;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class LoginUnitTest {

    CogniteClient cogniteClient;

    OkHttpClient httpClient;

    @Mock
    Call call;

    @BeforeEach
    void init() throws Exception {
        httpClient = Mockito.mock(OkHttpClient.class);

        AutoValue_CogniteClient.Builder builder = new AutoValue_CogniteClient.Builder();

        cogniteClient = builder.setHttpClient(httpClient)
                                .setClientConfig(ClientConfig.create())
                                .setAuthType(CogniteClient.AuthType.CLIENT_CREDENTIALS)
                                .setBaseUrl(TestConfigProvider.getHost())
                                .build();

        Mockito.when(httpClient.newCall(ArgumentMatchers.any()))
                .thenReturn(call);
    }

    @Test
    @DisplayName("logged in test")
    void testLoggedIn() throws Exception {
        Response response = MockResponseBodyGenerator.loginStatusLoggedInResponse();

        Mockito.when(call.execute())
                .thenReturn(response);

        LoginStatus loginStatus = cogniteClient.login().loginStatusByApiKey();

        Assertions.assertNotNull(loginStatus);

        Assertions.assertEquals(loginStatus.getUser(), "tom@example.com");
        Assertions.assertEquals(loginStatus.getLoggedIn(), true);
    }

    @Test
    @DisplayName("not logged in test")
    void testNotLoggedIn() throws Exception {
        Response response = MockResponseBodyGenerator.loginStatusNotLoggedInResponse();

        Mockito.when(call.execute())
                .thenReturn(response);

        LoginStatus loginStatus = cogniteClient.login().loginStatusByApiKey();

        Assertions.assertNotNull(loginStatus);

        Assertions.assertEquals(loginStatus.getUser(), "");
        Assertions.assertEquals(loginStatus.getLoggedIn(), false);
    }

}
