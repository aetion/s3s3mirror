package org.cobbzilla.s3s3mirror.store.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import org.cobbzilla.s3s3mirror.MirrorOptions;

import java.util.concurrent.atomic.AtomicReference;

public class S3ClientService {

    private static final AtomicReference<AmazonS3Client> s3client = new AtomicReference<AmazonS3Client>(null);

    public static AmazonS3Client getS3Client(MirrorOptions options) {
        synchronized (s3client) {
            if (s3client.get() == null) s3client.set(initS3Client(options));
        }
        return s3client.get();
    }

    public static AmazonS3Client initS3Client(MirrorOptions options) {

        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withProtocol((options.isSsl() ? Protocol.HTTPS : Protocol.HTTP))
                .withMaxConnections(options.getMaxConnections());

        if (options.getHasProxy()) {
            clientConfiguration = clientConfiguration
                    .withProxyHost(options.getProxyHost())
                    .withProxyPort(options.getProxyPort());
        }

        AWSCredentialsProviderChain awsCredentialProviders = options.getAwsCredentialProviders();
        if (awsCredentialProviders.getCredentials() == null)
            throw new IllegalStateException("Failed to obtain AWS credentials.");
        final AmazonS3Client client = new AmazonS3Client(awsCredentialProviders, clientConfiguration);
        if (options.hasEndpoint()) client.setEndpoint(options.getEndpoint());
        if (options.hasPathStyleAccess()) client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));

        return client;
    }

}