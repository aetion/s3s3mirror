package org.cobbzilla.s3s3mirror;

import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.slf4j.Logger;

public interface KeyJob extends Runnable {
    String getSource();
    String getDestination();
    Logger getLog();
    FileSummary getMetadata(String bucket, String key) throws Exception;
}
