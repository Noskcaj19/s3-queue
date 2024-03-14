package com.sikorsky;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.*;

import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Properties;

enum RequestStatus {
    QUEUED("queued"),
    DOWNLOADED("downloaded"),
    FAILED("failed");

    public final String value;

    RequestStatus(String value) {
        this.value = value;
    }
}

public class DownloadQueueClient {
    private final S3TransferManager s3TransferMgr;
    private final S3AsyncClient s3Client;
    private final Logger log = LogManager.getLogger(DownloadQueueClient.class);
    private final String hostname;
    private final Connection conn;
    private final Path baseDir;

    private Connection getConnection() throws SQLException {
        String url = "jdbc:postgresql://localhost/postgres";
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        // props.setProperty("password", "secret");
        props.setProperty("ssl", "false");

        return DriverManager.getConnection(url, props);
    }

    public void listenForever() throws SQLException {
        var stmt = conn.createStatement();
        stmt.execute("LISTEN remote_server_download_queue_updated");

        checkForRequests();
        while (true) {
            // wait until notifications exist
            var notifications = ((PGConnection) conn).getNotifications(0);
            for (PGNotification notification : notifications) {
                handleNotify(notification);
            }
        }
    }

    private void handleNotify(PGNotification notification) throws SQLException {
        var targetHostname = notification.getParameter();
        log.trace("currentHostname={} targetHostname={}", hostname, targetHostname);
        if (!hostname.equals(targetHostname)) {
            log.trace("hostname does not match, ignoring notification");
            return;
        }
        checkForRequests();
    }

    private void checkForRequests() throws SQLException {
        try (var stmt = conn.prepareStatement(
                "select download_id, url, name from remote_server_download_queue where target_hostname = ? and status = 'queued'")) {
            stmt.setString(1, hostname);
            var rs = stmt.executeQuery();
            while (rs.next()) {
                var id = rs.getLong(1);
                var url = rs.getString(2);
                var target = rs.getString(3);

                try {
                    downloadUrl(id, url, baseDir.resolve(target));
                } catch (Exception e) {
                    setRequestStatus(id, RequestStatus.FAILED);
                }
            }
        }
    }

    private void downloadUrl(long id, String url, Path target) {
        if (url.startsWith("s3:")) {
            downloadS3Url(id, url, target);
        } else if (url.startsWith("https") || url.startsWith("http")) {
            downloadHttpUrl(id, url, target);
        } else {
            log.error("id: {} Unknown URL type: \"{}\"", id, url);
        }
    }

    private Long doS3FileDownload(String bucketName,
            String key, Path downloadedFileWithPath) {
        DownloadFileRequest downloadFileRequest = DownloadFileRequest.builder()
                .getObjectRequest(b -> b.bucket(bucketName).key(key))
                .destination(downloadedFileWithPath)
                .build();

        FileDownload downloadFile = s3TransferMgr.downloadFile(downloadFileRequest);

        CompletedFileDownload downloadResult = downloadFile.completionFuture().join();
        return downloadResult.response().contentLength();
    }

    private Long doS3FolderDownload(String bucketName, String prefix, Path targetFolder) {
        S3TransferManager transferManager = S3TransferManager.builder().s3Client(s3Client).build();
        transferManager.close();
        DirectoryDownload directoryDownload = transferManager.downloadDirectory(
                DownloadDirectoryRequest.builder()
                        .destination(targetFolder)
                        .bucket(bucketName)
                        .listObjectsV2RequestTransformer(l -> l.prefix(prefix))
                        .build());

        CompletedDirectoryDownload completedDirectoryDownload = directoryDownload.completionFuture().join();

        completedDirectoryDownload.failedTransfers()
                .forEach(failedFileDownload -> log.error(failedFileDownload.request().getObjectRequest().key()));
        return 0L;
    }

    private void downloadS3Url(long id, String url, Path target) {
        log.info("id: {} Downloading S3 url: {}", id, url);
        var parsedUri = s3Client.utilities().parseUri(URI.create(url));
        var bucket = parsedUri.bucket().orElse(null);
        var key = parsedUri.key().orElse(null);
        if (bucket == null || key == null) {
            log.error("Failed to download S3 URL, bucket and key are not both non-null");
            return;
        }
        if (key.endsWith("/")) {
            doS3FolderDownload(bucket, key, target);
        } else {
            doS3FileDownload(bucket, key, target);
        }
        log.info("id: {} Download complete", id);
        setRequestStatus(id, RequestStatus.DOWNLOADED);
    }

    private void downloadHttpUrl(long id, String url, Path target) {
        log.info("id: {} Downloading http url: {}", id, url);

        try (BufferedInputStream in = new BufferedInputStream(new URL(url).openStream());
                FileOutputStream fileOutputStream = new FileOutputStream(target.toString())) {
            byte[] dataBuffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                fileOutputStream.write(dataBuffer, 0, bytesRead);
            }
        } catch (IOException e) {
            setRequestStatus(id, RequestStatus.FAILED);
            return;
        }
        log.debug("id: {} download complete", id);
        setRequestStatus(id, RequestStatus.DOWNLOADED);
    }

    private void setRequestStatus(long id, RequestStatus status) {
        try (var stmt = conn
                .prepareStatement("update remote_server_download_queue set status = ? where download_id = ?")) {
            stmt.setString(1, status.value);
            stmt.setLong(2, id);
            stmt.execute();
            var updated = stmt.getUpdateCount();

            if (updated != 1) {
                // TODO: What should happen if we get in this situation?
                log.error("Failed to mark request {} as {}", id, status);
            }
        } catch (SQLException e) {
            // TODO: What should happen if we get in this situation?

            log.error("An error occurred marking request {} as {}", id, status);
            log.error(e);
        }
    }

    private S3AsyncClient getS3Client() {
        return S3AsyncClient.crtBuilder()
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(
                                System.getenv("AWS_USERNAME"),
                                System.getenv("AWS_PASSWORD"))))
                .region(Region.US_EAST_1)
                .targetThroughputInGbps(20.0)
                .minimumPartSizeInBytes(8L * 1000 * 1000)
                .build();
    }

    private static S3TransferManager getS3TransferManager(S3AsyncClient s3AsyncClient) {
        S3TransferManager transferManager = S3TransferManager.builder()
                .s3Client(s3AsyncClient)
                .build();
        return transferManager;
    }

    DownloadQueueClient(String baseDir) throws UnknownHostException, SQLException {
        s3Client = getS3Client();
        s3TransferMgr = getS3TransferManager(s3Client);

        this.baseDir = Path.of(baseDir);
        hostname = InetAddress.getLocalHost().getHostName();
        conn = getConnection();
    }
}