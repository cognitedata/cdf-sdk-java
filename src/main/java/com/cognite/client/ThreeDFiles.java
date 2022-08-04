/*
 * Copyright (c) 2020 Cognite AS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognite.client;

import com.cognite.client.dto.ThreeDFileBinary;
import com.cognite.client.servicesV1.ConnectorServiceV1;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * This class represents the Cognite 3DFiles api endpoint.
 *
 * It provides methods for reading and writing.
 */
@AutoValue
public abstract class ThreeDFiles extends ApiBase {

    /**
     * Downloads 3D file binaries to a local / network path.
     *
     * Downloads a set of 3D file binaries based on {@code id}.
     *
     * Supported destination file stores for the file binary:
     * - Local (network) disk. Specify the temp path as {@code file://<host>/<my-path>/}.
     * Examples: {@code file://localhost/home/files/, file:///home/files/, file:///c:/temp/}
     * - Google Cloud Storage. Specify the temp path as {@code gs://<my-storage-bucket>/<my-path>/}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     Path downloadPath = Paths.get("");
     *     client
     *          .threeD()
     *          .files()
     *          .downloadToPath("threeDFileId", downloadPath);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/3D-Files/operation/get3DFile">API Reference - Retrieve a 3D file</a>
     *
     * @see CogniteClient
     * @see CogniteClient#threeD()
     * @see ThreeD#files()
     * @see #download(Long,Path,boolean)
     *
     * @param threeDFileId The id of the 3D file to download.
     * @param downloadPath The URI to the download storage
     * @return File containers with file headers and references/byte streams of the binary.
     */
    public ThreeDFileBinary downloadToPath(Long threeDFileId, Path downloadPath) throws Exception {
        return download(threeDFileId, downloadPath, false);
    }

    /**
     * Downloads 3D file binaries.
     *
     * Downloads a set of 3D file binaries based on {@code id}. The file
     * binaries can be downloaded as files or byte streams. In case the file is very large (> 200MB) it has to
     * be streamed directly to the file system (i.e. downloaded as a file).
     *
     * Supported destination file stores for the file binary:
     * - Local (network) disk. Specify the temp path as {@code file://<host>/<my-path>/}.
     * Examples: {@code file://localhost/home/files/, file:///home/files/, file:///c:/temp/}
     * - Google Cloud Storage. Specify the temp path as {@code gs://<my-storage-bucket>/<my-path>/}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     Path downloadPath = Paths.get("");
     *     client
     *          .threeD()
     *          .files()
     *          .download("threeDFileId", downloadPath, true);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/3D-Files/operation/get3DFile">API Reference - Retrieve a 3D file</a>
     *
     * @see CogniteClient
     * @see CogniteClient#threeD()
     * @see ThreeD#files()
     * @see #downloadFileBinary(Long,URI,boolean)
     *
     * @param threeDFileId The id of the 3D file to download.
     * @param downloadPath The URI to the download storage
     * @param preferByteStream Set to true to return byte streams when possible, set to false to always store
     *                         binary as file.
     * @return File containers with file headers and references/byte streams of the binary.
     */
    public ThreeDFileBinary download(Long threeDFileId, Path downloadPath, boolean preferByteStream) throws Exception {
        String loggingPrefix = getLoggingPrefix();
        Preconditions.checkArgument(java.nio.file.Files.isDirectory(downloadPath),
                loggingPrefix + "The download path must be a valid directory.");

        Instant startInstant = Instant.now();
        if (Objects.isNull(threeDFileId)) {
            LOG.warn("{} No 3D file id specified in the request. Will skip the download request.", loggingPrefix);
            return null;
        }

        ThreeDFileBinary fileBinary = downloadFileBinary(threeDFileId, downloadPath.toUri(), !preferByteStream);

        LOG.info(loggingPrefix + "Successfully downloaded 3D file with id {} within a duration of {}.",
                threeDFileId,
                Duration.between(startInstant, Instant.now()).toString());

        return fileBinary;
    }

    @NotNull
    private String getLoggingPrefix() {
        String loggingPrefix = "download() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        return loggingPrefix;
    }

    /**
     * Downloads 3D file binaries.
     *
     * This method is intended for advanced use cases, for example when using this SDK as a part of
     * a distributed system.
     *
     * Downloads a set of 3D file binaries based on {@code id}. The 3d file
     * binaries can be downloaded as files or byte streams. In case the file is very large (> 200MB) it has to
     * be streamed directly to the file system (to the temp storage area).
     *
     * Supported temp storage for the file binary:
     * - Local (network) disk. Specify the temp path as {@code file://<host>/<my-path>/}.
     * Examples: {@code file://localhost/home/files/, file:///home/files/, file:///c:/temp/}
     * - Google Cloud Storage. Specify the temp path as {@code gs://<my-storage-bucket>/<my-path>/}.
     *
     * <h2>Example:</h2>
     * <pre>
     * {@code
     *     Path downloadPath = Paths.get("").toUri();
     *     client
     *          .threeD()
     *          .files()
     *          .downloadFileBinary("threeDFileId", downloadPath, true);
     * }
     * </pre>
     *
     * <a href="https://docs.cognite.com/api/v1/#tag/3D-Files/operation/get3DFile">API Reference - Retrieve a 3D file</a>
     *
     * @see CogniteClient
     * @see CogniteClient#threeD()
     * @see ThreeD#files()
     *
     * @param threeDFileId The id of the 3D file to download.
     * @param tempStoragePath The URI to the download storage. Set to null to only perform in-memory download.
     * @param forceTempStorage Set to true to always download the binary to temp storage
     * @return The file binary.
     * @throws Exception
     */
    public ThreeDFileBinary downloadFileBinary(final Long threeDFileId,
                                               @Nullable final URI tempStoragePath,
                                               final boolean forceTempStorage) throws Exception {
        final int MAX_RETRIES = 3;
        String loggingPrefix = "downloadFileBinaries() - " + RandomStringUtils.randomAlphanumeric(5) + " - ";
        Preconditions.checkArgument(!(null == tempStoragePath && forceTempStorage),
                "Illegal parameter combination. You must specify a URI in order to force temp storage.");

        Instant startInstant = Instant.now();
        // do not send empty requests.
        if (Objects.isNull(threeDFileId)) {
            LOG.warn("{} No 3D file id specified in the request. Will skip the download request.", loggingPrefix);
            return null;
        }

        LOG.debug(loggingPrefix + "Start downloading 3D file with id {} at {}", threeDFileId, startInstant);

        /** Download file **/
        ThreeDFileBinary downloadResponse =
                executeFileBinariesDownload(threeDFileId, tempStoragePath, forceTempStorage);

        LOG.debug("{} Completed download 3D file with id {} at {}", loggingPrefix, threeDFileId, startInstant);

        return downloadResponse;
    }


    private ThreeDFileBinary executeFileBinariesDownload(Long threeDFileId,
                                                   @Nullable URI tempStoragePath,
                                                   boolean forceTempStorage) throws Exception {
        // Set up the download service
        ConnectorServiceV1.ThreeDFileBinaryReader reader = getClient().getConnectorService().readThreeDFileBinariesById()
                .enableForceTempStorage(forceTempStorage);

        if (null != tempStoragePath) {
            reader = reader.withTempStoragePath(tempStoragePath);
        }

        Request request = addAuthInfo(Request.create().withRootParameter("id", threeDFileId));

        return reader.readThreeDFileBinaries(request);
    }

    private static ThreeDFiles.Builder builder() {
        return new AutoValue_ThreeDFiles.Builder();
    }

    /**
     * Constructs a new {@link ThreeDFiles} object using the provided client configuration.
     *
     * This method is intended for internal use--SDK clients should always use {@link CogniteClient}
     * as the entry point to this class.
     *
     * @param client The {@link CogniteClient} to use for configuration settings.
     * @return the assets api object.
     */
    public static ThreeDFiles of(CogniteClient client) {
        return ThreeDFiles.builder()
                .setClient(client)
                .build();
    }

    @AutoValue.Builder
    abstract static class Builder extends ApiBase.Builder<ThreeDFiles.Builder> {
        abstract ThreeDFiles build();
    }
}
