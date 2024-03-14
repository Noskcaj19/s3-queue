package com.sikorsky;

import java.net.UnknownHostException;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args) throws SQLException, UnknownHostException {
        var downloadDir = System.getenv("DOWNLOAD_QUEUE_DIRECTORY");

        new DownloadQueueClient(downloadDir).listenForever();
    }
}
