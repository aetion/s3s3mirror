package org.cobbzilla.s3s3mirror.store.s3.job;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import org.cobbzilla.s3s3mirror.MirrorContext;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class S3Restore {
    private static final String STORAGE_CLASS_STANDARD = "STANDARD";
    private static final String STORAGE_CLASS_INTELLIGENT_TIERING = "INTELLIGENT_TIERING";

    private static final List<String> SUPPORTED_STORAGE_CLASSES = Arrays.asList(
            STORAGE_CLASS_STANDARD,
            STORAGE_CLASS_INTELLIGENT_TIERING);
    private static final List<String> SUPPROTED_ARCHIVE_STATUSES = Arrays.asList(
            "ARCHIVE_ACCESS",
            "DEEP_ARCHIVE_ACCESS");

    public static boolean checkRestoreRequired(
            ObjectMetadata sourceMetadata,
            String key,
            MirrorContext mirrorContext,
            AmazonS3Client s3client) {
        if (!mirrorContext.getOptions().intelligentTieringRestore()){
            return false;
        }

        String storageClass = Optional.ofNullable(sourceMetadata.getStorageClass())
                .orElse(STORAGE_CLASS_STANDARD);
        if (!SUPPORTED_STORAGE_CLASSES.contains(storageClass)) {
            throw new IllegalStateException("Failed copy " + key + " because it is stored using the " + storageClass + " storage class");
        }
        Optional<String> archiveStatus = Optional.ofNullable(sourceMetadata.getArchiveStatus());
        if (STORAGE_CLASS_INTELLIGENT_TIERING.equals(storageClass)
                && archiveStatus.isPresent()) {
            // Object is archived we will init restore
            if(!SUPPROTED_ARCHIVE_STATUSES.contains(archiveStatus.get())){
                throw new IllegalStateException("Failed copy " + key + " because it is stored " +
                        "using the " + storageClass + " storage class and " +
                        "has an unsupported archive status of " + archiveStatus.get());
            }
            Optional<Boolean> ongoingRestore = Optional.ofNullable(sourceMetadata.getOngoingRestore());
            if(!ongoingRestore.orElse(false)){
                s3client.restoreObjectV2(new RestoreObjectRequest(
                        mirrorContext.getOptions().getSourceBucket(), key));
            }
            mirrorContext.getStats().s3restoreCount.incrementAndGet();
            return true;
        }
        return false;
    }
}
