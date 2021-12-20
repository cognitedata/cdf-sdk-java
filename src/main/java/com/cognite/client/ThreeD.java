package com.cognite.client;

import com.google.auto.value.AutoValue;

@AutoValue
public abstract class ThreeD extends ApiBase {

    private static ThreeD.Builder builder() {
        return new AutoValue_ThreeD.Builder();
    }

    /**
     * Constructs a new {@link ThreeD} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static ThreeD of(CogniteClient client) {
        return ThreeD.builder()
                .setClient(client)
                .build();
    }

    /**
     * Returns {@link ThreeDModels} representing 3D Models api endpoints.
     *
     * @return The ThreeDModels api endpoints.
     */
    public ThreeDModels models() {
        return ThreeDModels.of(getClient());
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<ThreeD.Builder> {
        abstract ThreeD build();
    }
}
