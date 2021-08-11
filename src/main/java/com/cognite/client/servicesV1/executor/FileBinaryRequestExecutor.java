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

package com.cognite.client.servicesV1.executor;

import com.cognite.client.dto.FileBinary;
import com.cognite.client.servicesV1.ConnectorConstants;
import com.cognite.client.servicesV1.ResponseBinary;
import com.google.auto.value.AutoValue;
import com.google.cloud.storage.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import okhttp3.*;
import okhttp3.internal.http2.StreamResetException;
import okio.BufferedSink;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLProtocolException;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * This request executor implements specific behavior to deal with very large request/response bodies when
 * operating on file binaries. It will cap in-memory bodies at 200MiB and use temporary
 * blob storage to host the binary. This "overflow to disk" behavior is supported for both downloads
 * and uploads.
 *
 * This class will execute an okhttp3 request on a separate thread and publish the result via a
 * <code>CompletableFuture</code>. This allows the client code to spin off multiple concurrent request without blocking
 * the main thread.
 *
 * This represents the "blocking IO on a separate thread" pattern, and will work fine for client workloads (limited
 * number of concurrent requests).
 */
@AutoValue
public abstract class FileBinaryRequestExecutor {
    // Valid response codes *outside* the 200-range.
    private static final ImmutableList<Integer> DEFAULT_VALID_RESPONSE_CODES = ImmutableList.of();

    private static final ImmutableList<Integer> RETRYABLE_RESPONSE_CODES = ImmutableList.of(
            408,    // request timeout
            429,    // too many requests
            500,    // internal server error
            502,    // bad gateway
            503,    // service unavailable
            504     // gateway timeout
    );

    private static final int DEFAULT_NUM_WORKERS = 8;
    //private static final ForkJoinPool DEFAULT_POOL = new ForkJoinPool(DEFAULT_NUM_WORKERS);
    private static final ExecutorService DEFAULT_POOL = Executors.newFixedThreadPool(DEFAULT_NUM_WORKERS);

    protected final static Logger LOG = LoggerFactory.getLogger(FileBinaryRequestExecutor.class);

    private final String randomIdString = RandomStringUtils.randomAlphanumeric(5);
    private final String loggingPrefix = "FileBinaryRequestExecutor [" + randomIdString + "] -";

    private static Builder builder() {
        return new AutoValue_FileBinaryRequestExecutor.Builder()
                .setExecutor(DEFAULT_POOL)
                .setMaxRetries(ConnectorConstants.DEFAULT_MAX_RETRIES)
                .setValidResponseCodes(DEFAULT_VALID_RESPONSE_CODES)
                .setForceTempStorage(false)
                .setDeleteTempFile(true);
    }

    public static FileBinaryRequestExecutor of(OkHttpClient client) {
        Preconditions.checkNotNull(client, "Http client cannot be null.");

        return FileBinaryRequestExecutor.builder()
                .setHttpClient(client)
                .build();
    }

    abstract Builder toBuilder();
    abstract OkHttpClient getHttpClient();
    abstract List<Integer> getValidResponseCodes();
    abstract Executor getExecutor();
    abstract int getMaxRetries();
    abstract boolean isForceTempStorage();
    abstract boolean isDeleteTempFile();

    @Nullable
    abstract URI getTempStoragePath();

    /**
     * Sets the executor to use for running the api requests.
     *
     * The default executor is a <code>ForkJoinPool</code> with a target parallelism of four threads per core.
     * @param executor
     * @return
     */
    public FileBinaryRequestExecutor withExecutor(Executor executor) {
        Preconditions.checkNotNull(executor, "Executor cannot be null.");
        return toBuilder().setExecutor(executor).build();
    }

    /**
     * Sets the maximum number of retries.
     *
     * The default setting is 3.
     * @param retries
     * @return
     */
    public FileBinaryRequestExecutor withMaxRetries(int retries) {
        Preconditions.checkArgument(retries <= ConnectorConstants.MAX_MAX_RETRIES
                        && retries >= ConnectorConstants.MIN_MAX_RETRIES,
                "Max retries out of range. Must be between "
                            + ConnectorConstants.MIN_MAX_RETRIES + " and " + ConnectorConstants.MAX_MAX_RETRIES);
        return toBuilder().setMaxRetries(retries).build();
    }

    /**
     * Specifies a set of valid http response codes *in addition* to the 200-range.
     *
     * By default, any 2xx response is considered a valid response. By specifying additional codes, this executor
     * will return responses from outside the 200-range. This could be useful in case you want to handle non-200
     * responses with custom logic. For example, duplicate detection and constraint violations are reported
     * as non-200 responses from the Cognite API.
     *
     * @param validResponseCodes A list of valid response codes.
     * @return
     */
    public FileBinaryRequestExecutor withValidResponseCodes(List<Integer> validResponseCodes) {
        Preconditions.checkNotNull(validResponseCodes, "Valid response codes cannot be null.");
        return toBuilder().setValidResponseCodes(validResponseCodes).build();
    }

    /**
     * Forces the use of temp storage for all binaries--not just the >200MiB ones.
     *
     * The default is {@code false}.
     *
     * @param enable
     * @return
     */
    public FileBinaryRequestExecutor enableForceTempStorage(boolean enable) {
        return toBuilder().setForceTempStorage(enable).build();
    }

    /**
     * Sets the temporary storage path for storing large file binaries. If the binary is >200 MiB it will be
     * stored in temp storage instead of in memory.
     *
     * The following storage providers are supported:
     * - Google Cloud Storage. Specify the temp path as {@code gs://<my-storage-bucket>/<my-path>/}.
     *
     * @param path
     * @return
     */
    public FileBinaryRequestExecutor withTempStoragePath(URI path) {
        Preconditions.checkNotNull(path,
                "Temp storage path cannot be null or empty.");
        Preconditions.checkArgument(validateTempStoragePath(path),
                "Invalid storage path: " + path);
        return toBuilder().setTempStoragePath(path).build();
    }

    /**
     * Configure how to treat a temp blob after an upload. This setting only affects behavior when uploading
     * file binaries to the Cognite API--it has no effect on downloading file binaries.
     *
     * When set to {@code true}, the temp file (if present) will be removed after a successful upload. If the file
     * binary is memory-based (which is the default for small and medium sized files), this setting has no effect.
     *
     * When set to {@code false}, the temp file (if present) will not be deleted.
     *
     * The default setting is {@code true}.
     *
     * @param enable
     * @return
     */
    public FileBinaryRequestExecutor enableDeleteTempFile(boolean enable) {
        return toBuilder().setDeleteTempFile(enable).build();
    }

    /**
     * Executes a given request and returns the response body as a file binary. Checks for transient server errors
     * and retires the request until a valid response is produced, or the max number of retries is reached.
     * This method executes as a blocking I/O operation on a separate thread--the calling thread is not blocked
     * and can continue working on its tasks.
     *
     * Each retry is performed with exponential back-off in case the api is overloaded.
     *
     * If no valid response can be produced, this method will throw an exception.
     *
     * @param request The request to execute
     * @return
     */
    public CompletableFuture<FileBinary> downloadBinaryAsync(Request request) {
        LOG.debug(loggingPrefix + "Executing request async. Detected {} CPUs. Default executor running with "
                + "a target parallelism of {}", Runtime.getRuntime().availableProcessors(), DEFAULT_NUM_WORKERS);

        CompletableFuture<FileBinary> completableFuture = new CompletableFuture<>();
        getExecutor().execute((Runnable & CompletableFuture.AsynchronousCompletionTask) () -> {
            try {
                completableFuture.complete(this.downloadBinary(request));
            } catch (Exception e) {
                completableFuture.completeExceptionally(e);
            }
        });

        return completableFuture;
    }

    /**
     * Executes a given request and returns the response body as a file binary. Checks for transient server errors
     * and retires the request until a valid response is produced, or the max number of retries is reached.
     * This method blocks until a <code>Response</code> is produced.
     *
     * Each retry is performed with exponential back-off in case the api is overloaded.
     *
     * If no valid response can be produced, this method will throw an exception.
     *
     * The async version of this method is <code>downloadBinaryAsync</code>
     *
     * */
    public FileBinary downloadBinary(Request request) throws Exception {
        LOG.debug(loggingPrefix + "Executing request to [{}]", request.url().toString());

        List<Exception> catchedExceptions = new ArrayList<>();
        int responseCode = -1;

        ThreadLocalRandom random = ThreadLocalRandom.current();
        // progressive back off in case of retries.
        for (int callNo = 0;
             callNo < this.getMaxRetries();
             callNo++,
                     Thread.sleep(Math.min(32000L, (500L * (long) Math.exp(callNo)) + random.nextLong(1000)))) {

            try (Response response = getHttpClient().newCall(request).execute()) {
                responseCode = response.code();
                LOG.debug(loggingPrefix + "Response received with response code {}", responseCode);

                // if the call was not successful, throw an error
                if (!response.isSuccessful() && !getValidResponseCodes().contains(responseCode)) {
                    throw new Exception(loggingPrefix +
                            "Unexpected response code when reading: " + responseCode + ". "
                                    + response.toString() + System.lineSeparator()
                                    + "Response content length: " + response.body().contentLength() + System.lineSeparator()
                                    + "Response headers: " + response.headers().toString() + System.lineSeparator()
                    );
                }
                // check the response
                if (response.body() == null) {
                    throw new Exception(loggingPrefix + "Successful response, but the body is null. "
                                                + response.toString() + System.lineSeparator()
                                                + "Response headers: " + response.headers().toString());
                }

                // check the content length. When downloading files >200MiB we will use temp storage.
                if (response.body().contentLength() > (1024L * 1024L * 200L) || isForceTempStorage()) {
                    if (null == getTempStoragePath()) {
                        String message = String.format("File too large to download to memory. Consider configuring temp "
                                        + "storage on the file reader.%n"
                                        + "Content-length = [%d]. %n"
                                        + "Response headers: %s",
                                response.body().contentLength(),
                                response.headers().toString());
                        throw new IOException(message);
                    }
                    LOG.info("Downloading {} MiB to temp storage binary.",
                            String.format("%.2f", response.body().contentLength() / (1024d * 1024d)));
                    return downloadBinaryToTempStorage(response);
                } else {
                    LOG.info("Downloading {} MiB to memory-based binary.",
                            String.format("%.2f", response.body().contentLength() / (1024d * 1024d)));
                    return FileBinary.newBuilder()
                            .setBinary(ByteString.readFrom(response.body().byteStream()))
                            .setContentLength(response.body().contentLength())
                            .build();
                }
            } catch (Exception e) {
                catchedExceptions.add(e);

                // if we get a transient error, retry the call
                if (e instanceof java.net.SocketTimeoutException
                        || e instanceof StreamResetException
                        || e instanceof SSLProtocolException
                        || e instanceof SSLException
                        || RETRYABLE_RESPONSE_CODES.contains(responseCode)) {
                    LOG.warn(loggingPrefix + "Transient error when downloading file ("
                            + ", response code: " + responseCode
                            + "). Retrying...", e);

                } else {
                    // not transient, just re-throw
                    LOG.error(loggingPrefix + "Non-transient error occurred when downloading file."
                            + ", Response code: " + responseCode, e);
                    throw e;
                }
            }
        }

        // No results are produced. Throw the list of registered Exception.
        String exceptionMessage = String.format("Unable to download file binary from %s.",
                request.url().toString());
        if (catchedExceptions.size() > 0) { //add the details of the most recent exception.
            exceptionMessage += System.lineSeparator();
            exceptionMessage += catchedExceptions.get(catchedExceptions.size() -1).getMessage();
        }
        Exception e = new Exception(exceptionMessage);
        catchedExceptions.forEach(e::addSuppressed);
        throw e;
    }

    /**
     * Executes a given request and returns the response body as a file binary. Checks for transient server errors
     * and retires the request until a valid response is produced, or the max number of retries is reached.
     * This method executes as a blocking I/O operation on a separate thread--the calling thread is not blocked
     * and can continue working on its tasks.
     *
     * Each retry is performed with exponential back-off in case the api is overloaded.
     *
     * If no valid response can be produced, this method will throw an exception.
     *
     */
    public CompletableFuture<ResponseBinary> uploadBinaryAsync(FileBinary fileBinary, URL targetURL) {
        LOG.debug(loggingPrefix + "Executing request async. Detected {} CPUs. Default executor running with "
                + "a target parallelism of {}", Runtime.getRuntime().availableProcessors(), DEFAULT_NUM_WORKERS);

        CompletableFuture<ResponseBinary> completableFuture = new CompletableFuture<>();
        getExecutor().execute((Runnable & CompletableFuture.AsynchronousCompletionTask) () -> {
            try {
                completableFuture.complete(this.uploadBinary(fileBinary, targetURL));
            } catch (Exception e) {
                completableFuture.completeExceptionally(e);
            }
        });

        return completableFuture;
    }

    /**
     * Writes a file binary to the target URL. Supports both in-memory bytes streams and temp
     * storage blobs. Checks for transient server errors
     * and retires the request until a valid response is produced, or the max number of retries is reached.
     * This method blocks until a <code>Response</code> is produced.
     *
     * Each retry is performed with exponential back-off in case the api is overloaded.
     *
     * If no valid response can be produced, this method will throw an exception.
     *
     * The async version of this method is <code>uploadBinaryAsync</code>
     *
     * */
    public ResponseBinary uploadBinary(FileBinary fileBinary, URL targetURL) throws Exception {
        LOG.debug(loggingPrefix + "Executing request to [{}]", targetURL.toString());

        // check all preconditions
        Preconditions.checkState(fileBinary.getBinaryTypeCase() !=
                        FileBinary.BinaryTypeCase.BINARYTYPE_NOT_SET,
                "The file binary is not set.");
        Preconditions.checkState(!(fileBinary.getBinaryTypeCase() == FileBinary.BinaryTypeCase.BINARY
                        && fileBinary.getBinary().isEmpty()),
                "The file binary is empty.");
        Preconditions.checkState(!(fileBinary.getBinaryTypeCase() == FileBinary.BinaryTypeCase.BINARY_URI
                        && fileBinary.getBinaryUri().isEmpty()),
                "The file binary URI is empty.");

        HttpUrl url = HttpUrl.get(targetURL);
        Preconditions.checkState(null != url, "Upload URL not valid: " + targetURL.toString());

        // build request
        String mimeType = "application/octet-stream"; // The default if none is specified
        Request request;

        if (fileBinary.getBinaryTypeCase() == FileBinary.BinaryTypeCase.BINARY) {
            LOG.debug(loggingPrefix + "File binary sourced from in-memory storage");
            int contentLength = fileBinary.getBinary().size();

            request = new Request.Builder()
                    .url(url)
                    .header("Content-length", String.valueOf(contentLength))
                    .post(RequestBody.create(fileBinary.getBinary().toByteArray(), MediaType.get(mimeType)))
                    .build();
        } else {
            LOG.debug(loggingPrefix + "File binary sourced temporary blob storage: " + fileBinary.getBinaryUri());
            request = new Request.Builder()
                    .url(url)
                    .post(new UploadFileBinaryRequestBody(fileBinary.getBinaryUri(),
                            MediaType.get(mimeType),
                            isDeleteTempFile()))
                    .build();
        }

        // execute request
        Long apiLatency = 0L;
        int apiRetryCounter = 0;

        List<Exception> catchedExceptions = new ArrayList<>();
        int responseCode = -1;
        String requestId = "";
        Long timerStart;

        ThreadLocalRandom random = ThreadLocalRandom.current();
        // progressive back off in case of retries.
        for (int callNo = 0;
             callNo < this.getMaxRetries();
             callNo++,
                     Thread.sleep(Math.min(32000L, (500L * (long) Math.exp(callNo)) + random.nextLong(1000)))) {
            timerStart = System.currentTimeMillis();

            try (Response response = getHttpClient().newCall(request).execute()) {
                apiLatency = (System.currentTimeMillis() - timerStart);
                responseCode = response.code();
                LOG.debug(loggingPrefix + "Response received with response code {}",
                        responseCode);

                // if the call was not successful, throw an error
                if (!response.isSuccessful() && !getValidResponseCodes().contains(responseCode)) {
                    throw new IOException(
                            "Uploading file binary: Unexpected response code: " + responseCode + ". "
                            + response.toString() + System.lineSeparator()
                            + "Response body: " + response.body().string() + System.lineSeparator()
                            + "Response headers: " + response.headers().toString() + System.lineSeparator()
                    );
                }
                // check the response
                if (response.body() == null) {
                    throw new Exception("Uploading file binary: Successful response, but the body is null. "
                            + response.toString() + System.lineSeparator()
                            + "Response headers: " + response.headers().toString());
                }

                // check the response content length. When downloading very large files this may exceed 4GB
                // we put a limit of 500MiB on the response
                if (response.body().contentLength() > (1024L * 1024L * 500L)) {
                    String message = String.format("Response too large. "
                                    + "Content-length = [%d]. %n"
                                    + "Response headers: %s",
                            response.body().contentLength(),
                            response.headers().toString());
                    throw new IOException(message);
                }

                return ResponseBinary.of(response, ByteString.readFrom(response.body().byteStream()))
                        .withApiLatency(apiLatency)
                        .withApiRetryCounter(apiRetryCounter);
            } catch (Exception e) {
                catchedExceptions.add(e);

                // if we get a transient error, retry the call
                if (e instanceof java.net.SocketTimeoutException
                        || e instanceof StreamResetException
                        || e instanceof SSLProtocolException
                        || e instanceof SSLException
                        || RETRYABLE_RESPONSE_CODES.contains(responseCode)) {
                    apiRetryCounter++;
                    LOG.warn(loggingPrefix + "Transient error when reading from Fusion (request id: " + requestId
                            + ", response code: " + responseCode
                            + "). Retrying...", e);

                } else {
                    // not transient, just re-throw
                    LOG.error(loggingPrefix + "Non-transient error occurred when reading from Fusion. Request id: " + requestId
                            + ", Response code: " + responseCode, e);
                    throw e;
                }
            }
        }

        // No results are produced. Throw the list of registered Exception.
        String exceptionMessage = String.format("Unable to upload file binary to %s.",
                request.url().toString());
        if (catchedExceptions.size() > 0) { //add the details of the most recent exception.
            exceptionMessage += System.lineSeparator();
            exceptionMessage += catchedExceptions.get(catchedExceptions.size() -1).getMessage();
        }
        Exception e = new Exception(exceptionMessage);
        catchedExceptions.forEach(e::addSuppressed);
        throw e;
    }

    /*
    Download the response body to temp storage and return a FileBinary containing the URI reference
    to the temp file.
     */
    private FileBinary downloadBinaryToTempStorage(Response response) throws Exception {
        Preconditions.checkState(null != getTempStoragePath(),
                "Invalid temp storage path");
        ZonedDateTime nowUTC = Instant.now().atZone(ZoneId.of("UTC"));
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss");
        String tempFileName = new StringBuilder(36)
                .append("tempFile-")
                .append(nowUTC.format(format) + "-")
                .append(RandomStringUtils.randomAlphanumeric(7))
                .append(".tmp")
                .toString();

        LOG.debug(loggingPrefix + "Start downloading binary to temp storage: {}",
                tempFileName);

        if (null != getTempStoragePath().getScheme()
                && getTempStoragePath().getScheme().equalsIgnoreCase("gs")) {
            // Handler for Google Cloud Storage based blobs
            String bucketName = getTempStoragePath().getHost();
            String path = getTempStoragePath().getPath();
            if (!path.endsWith("/")) path = path + "/";         // make sure the path ends with a forward slash
            if (path.startsWith("/")) path = path.substring(1); // make sure the path does not start with a forward slash
            String objectName = path + tempFileName;
            LOG.debug(loggingPrefix + "Download to GCS: {}",
                    objectName);

            Storage cloudStorage = StorageOptions.getDefaultInstance().getService();
            BlobId blobId = BlobId.of(bucketName, objectName);
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
                    .setContentType("application/octet-stream")
                    .build();
            FileBinaryRequestExecutor.transferBytes(response.body().source(),
                    cloudStorage.writer(blobInfo),
                    (1024 * 4));

            URI fileURI = new URI("gs", bucketName, "/" + objectName, null);
            LOG.debug(loggingPrefix + "Finished downloading to GCS. Temp file URI: {}",
                    fileURI.toString());

            return FileBinary.newBuilder()
                    .setBinaryUri(fileURI.toString())
                    .setContentLength(response.body().contentLength())
                    .build();
        } else if (null != getTempStoragePath().getScheme()
                && getTempStoragePath().getScheme().equalsIgnoreCase("file")) {
            // Handler for local (or network) based file system blobs
            Files.createDirectories(Paths.get(getTempStoragePath()));
            Path tempFilePath = Paths.get(getTempStoragePath()).resolve(tempFileName);
            LOG.debug(loggingPrefix + "Download to local storage: {}",
                    tempFilePath.toAbsolutePath().toString());

            Files.createFile(tempFilePath);
            FileBinaryRequestExecutor.transferBytes(response.body().source(),
                    Files.newByteChannel(tempFilePath, StandardOpenOption.WRITE),
                    (1024 * 4));
            LOG.debug(loggingPrefix + "Finished downloading to local storage. Temp file URI: {}",
                    tempFilePath.toUri().toString());

            return FileBinary.newBuilder()
                    .setBinaryUri(tempFilePath.toUri().toString())
                    .setContentLength(response.body().contentLength())
                    .build();
        } else {
            throw new IOException("Temp storage location not supported: " + getTempStoragePath());
        }
    }

    /*
    Verifies that the temp storage URI is supported.
    Supported blob storage providers:
    - Google Cloud Storage
    - Local (network) file storage
     */
    private boolean validateTempStoragePath(URI path) {
        boolean validURI = false;

        // Validate Google Cloud Storage URIs
        if (null != path.getScheme() && path.getScheme().equalsIgnoreCase("gs")
                && null != path.getHost() && path.getHost().length() > 1) {
            validURI = true;
        }

        // Validate local (or network based) file system URI
        if (null != path.getScheme() && path.getScheme().equalsIgnoreCase("file")) {
            try {
                // To to create the temp directory
                Files.createDirectories(Paths.get(path));
                validURI = true;
            } catch (Exception e) {
                LOG.error(loggingPrefix + "Cannot access temp directory: " + path.toString());
                validURI = false;
            }
        }

        return validURI;
    }

    /*
    Transfers all bytes from a readable channel to a writeable channel. This method is used by Java.nio based
    streaming transfers for high performance IO.
     */
    private static void transferBytes(ReadableByteChannel input,
                                      WritableByteChannel output,
                                      int bufferSize) throws Exception{
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        buffer.clear();

        while (input.read(buffer) != -1) {
            buffer.flip();
            while (buffer.hasRemaining()) {
                output.write(buffer);
            }
            buffer.clear();
        }
        input.close();
        output.close();
    }

    /*
    RequestBody for streaming bytes from temp storage to the destination.
     */
    private static class UploadFileBinaryRequestBody extends RequestBody {
        private final URI fileURI;
        private final MediaType contentType;
        private final boolean deleteTempFile;

        private Storage cloudStorage = null;

        public UploadFileBinaryRequestBody(String binaryURI,
                                           MediaType contentType,
                                           boolean deleteTempFile) throws java.net.URISyntaxException {
            this.fileURI = new URI(binaryURI);
            this.contentType = contentType;
            this.deleteTempFile = deleteTempFile;
        }

        @Override
        public MediaType contentType() {
            return contentType;
        }

        /**
         * Writes the file byte stream from the specified URI to the request target. Performs streaming writes.
         *
         * @param bufferedSink
         * @throws IOException
         */
        @Override
        public void writeTo(@NotNull BufferedSink bufferedSink) throws IOException {
            if (null != fileURI.getScheme() && fileURI.getScheme().equalsIgnoreCase("gs")) {
                Blob blob = getBlob(fileURI);
                if (null == blob) {
                    LOG.error("Looks like the GCS blob is null/does not exist. File URI: {}",
                            fileURI.toString());
                    throw new IOException(String.format("Looks like the GCS blob is null/does not exist. File URI: %s",
                            fileURI.toString()));
                }
                blob.downloadTo(bufferedSink.outputStream());
                if (deleteTempFile) {
                    blob.delete();
                }
            } else if (null != fileURI.getScheme() && fileURI.getScheme().equalsIgnoreCase("file")) {
                // Handler for local (or network) based file system blobs
                if (!Files.isReadable(Paths.get(fileURI)) || !Files.isRegularFile(Paths.get(fileURI))) {
                    throw new IOException("Temp file is not a readable file: " + fileURI.toString());
                }
                SeekableByteChannel tempFileChannel = Files.newByteChannel(Paths.get(fileURI), StandardOpenOption.READ);
                try {
                    FileBinaryRequestExecutor.transferBytes(tempFileChannel,
                            bufferedSink,
                            (1024 * 4));
                    if (deleteTempFile) {
                        Files.delete(Paths.get(fileURI));
                    }
                } catch (Exception e) {
                    throw new IOException(e);
                }
            } else {
                throw new IOException("URI is unsupported: " + fileURI.toString());
            }
        }

        public long contentLength() {
            long contentLength = -1;
            if (fileURI.getScheme().equalsIgnoreCase("gs")) {
                try {
                    Blob blob = getBlob(fileURI);
                    if (null == blob) {
                        LOG.warn("Looks like the GCS blob is null/does not exist. File URI: {}",
                                fileURI.toString());
                    } else {
                        contentLength = blob.getSize();
                    }
                } catch (IOException e) {
                    LOG.warn(e.getMessage());
                }
            }

            return contentLength;
        }

        /*
        Gets the GCS blob from a GCS URI. Can be used to access blob metadata and start upload/download.
         */
        private Blob getBlob(URI fileURI) throws IOException {
            if (!fileURI.getScheme().equalsIgnoreCase("gs")) {
                throw new IOException("URI is not a valid GCS URI: " + fileURI.toString());
            }

            String bucketName = fileURI.getHost();
            String path = fileURI.getPath();
            if (path.startsWith("/")) path = path.substring(1); // make sure the path does not start with a forward slash

            BlobId blobId = BlobId.of(bucketName, path);
            return getCloudStorage().get(blobId);
        }

        /*
        Ensure a single instantiation of the GCS service.
         */
        private Storage getCloudStorage() {
            if (null == cloudStorage) {
                cloudStorage = StorageOptions.getDefaultInstance().getService();
            }
            return cloudStorage;
        }
    }

    @AutoValue.Builder
    public abstract static class Builder {
        abstract Builder setExecutor(Executor value);
        abstract Builder setHttpClient(OkHttpClient value);
        abstract Builder setValidResponseCodes(List<Integer> value);
        abstract Builder setMaxRetries(int value);
        abstract Builder setForceTempStorage(boolean value);
        abstract Builder setTempStoragePath(URI value);
        abstract Builder setDeleteTempFile(boolean value);

        abstract FileBinaryRequestExecutor autoBuild();

        public FileBinaryRequestExecutor build() {
            FileBinaryRequestExecutor requestExecutor = autoBuild();
            Preconditions.checkState(requestExecutor.getMaxRetries() <= ConnectorConstants.MAX_MAX_RETRIES
                            && requestExecutor.getMaxRetries() >= ConnectorConstants.MIN_MAX_RETRIES
                    , "Max retries out of range. Must be between "
                            + ConnectorConstants.MIN_MAX_RETRIES + " and " + ConnectorConstants.MAX_MAX_RETRIES);

            return requestExecutor;
        }
    }
}
