package org.cobbzilla.s3s3mirror.store.local.job;

import com.amazonaws.services.s3.AmazonS3Client;
import lombok.extern.slf4j.Slf4j;
import org.cobbzilla.s3s3mirror.KeyDeleteJob;
import org.cobbzilla.s3s3mirror.MirrorContext;
import org.cobbzilla.s3s3mirror.MirrorOptions;
import org.cobbzilla.s3s3mirror.store.FileSummary;
import org.cobbzilla.s3s3mirror.store.local.LocalFileStore;
import org.cobbzilla.s3s3mirror.store.s3.S3FileListing;
import org.cobbzilla.s3s3mirror.store.s3.S3FileStore;
import org.slf4j.Logger;

import java.io.File;

@Slf4j
public class LocalKeyDeleteJob extends KeyDeleteJob {

    @Override public Logger getLog() { return log; }

    public LocalKeyDeleteJob(AmazonS3Client client, MirrorContext context, FileSummary summary, Object notifyLock) {
        super(client, context, summary, notifyLock);
    }

    @Override
    public FileSummary getMetadata(String bucket, String key) throws Exception {
        return S3FileListing.buildSummary(key, S3FileStore.getObjectMetadata(bucket, key, context, s3client));
    }

    @Override protected boolean deleteFile(String bucket, String key) throws Exception {
        final MirrorOptions options = context.getOptions();

        final File destFile = LocalFileStore.getFileAndCreateParent(options.getDestinationBucket(), key);

        return destFile.delete();
    }

}
