package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import lombok.Getter;
import org.cobbzilla.s3s3mirror.stats.MirrorStats;
import org.cobbzilla.s3s3mirror.store.FileSummary;

public abstract class KeyDeleteJob implements KeyJob {

    protected AmazonS3Client s3client;
    protected final MirrorContext context;
    protected final FileSummary summary;
    protected final Object notifyLock;

    protected String keySource;
    @Getter
    protected String keyDestination;

    public KeyDeleteJob(AmazonS3Client client, MirrorContext context, FileSummary summary, Object notifyLock) {
        this.s3client = client;
        this.context = context;
        this.summary = summary;
        this.notifyLock = notifyLock;
        this.keyDestination = summary.getKey();

        final MirrorOptions options = context.getOptions();
        if (options.hasDestPrefix()) {
            // Note: in this case source file is as destination file from copy job.
            // so we use dest prefix here.
            String key_suffix = keyDestination.substring(options.getDestPrefixLength());
            keySource = options.getPrefix() + key_suffix;
        }
    }

    @Override
    public String getSource() {
        return keySource;
    }

    @Override
    public String getDestination() {
        return keyDestination;
    }

    protected abstract boolean deleteFile(String bucket, String key) throws Exception;

    @Override
    public void run() {
        final MirrorOptions options = context.getOptions();
        final MirrorStats stats = context.getStats();
        final boolean verbose = options.isVerbose();
        final int maxRetries = options.getMaxRetries();
        final String key = summary.getKey();
        try {
            if (!shouldDelete()) return;

            if (options.isDryRun()) {
                getLog().info("Would have deleted "+key+" from destination because "+ keySource +" does not exist in source");
            } else {
                boolean deletedOK = false;
                for (int tries=0; tries<maxRetries; tries++) {
                    if (verbose) getLog().info("deleting (try #"+tries+"): "+key);
                    try {
                        deletedOK = deleteFile(options.getDestinationBucket(), key);
                        if (verbose) getLog().info("successfully deleted (on try #"+tries+"): "+key);
                        break;

                    } catch (Exception e) {
                        getLog().error("unexpected exception deleting (try #"+tries+") "+key+": "+e);
                    }
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        getLog().error("interrupted while waiting to retry key: "+key);
                        break;
                    }
                }
                if (deletedOK) {
                    stats.objectsDeleted.incrementAndGet();
                } else {
                    stats.addErroredDelete(this);
                }
            }

        } catch (Exception e) {
            getLog().error("error deleting key: "+key+": "+e);
            if (!options.isDryRun()) context.getStats().addErroredDelete(this);

        } finally {
            synchronized (notifyLock) {
                notifyLock.notifyAll();
            }
            if (verbose) getLog().info("done with "+key);
        }
    }

    private boolean shouldDelete() {

        final MirrorOptions options = context.getOptions();

        // Does it exist in the source bucket
        try {
            final FileSummary metadata = getMetadata(options.getSourceBucket(), keySource);
            if (metadata == null) {
                if (options.isVerbose()) getLog().info("Key not found in source bucket (will delete from destination): " + keySource);
                return true;
            }
        } catch (Exception e) {
            getLog().warn("Error getting metadata for " + options.getSourceBucket() + "/" + keySource + " (not deleting): " + e);
        }
        return false;
    }

}
