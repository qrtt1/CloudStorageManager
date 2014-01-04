package org.qty.cloud.storage.aws.s3;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.qty.cloud.storage.Constants;
import org.qty.cloud.storage.IFile;
import org.qty.cloud.storage.IStorage;
import org.qty.cloud.storage.StorageOperationException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class AwsS3Storage implements IStorage {

    public static final String AWS_S3_ACCESS_ID = "aws.s3.accessId";
    public static final String AWS_S3_SECRET = "aws.s3.secret";
    public static final String AWS_S3_BUCKET = "aws.s3.bucket";

    String bucket;
    AmazonS3 client;

    public AwsS3Storage(final Properties settings) {
        try {
            initializeS3Settings(settings);
        } catch (Exception e) {
            throw new StorageOperationException(e);
        }
    }

    @Override
    public IFile get(String path) {
        return new AwsS3File(this, path);
    }

    @Override
    public void put(String path, IFile file) {
        InputStream input = null;

        try {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(file.length());
            Map<String, String> userMetadata = new HashMap<String, String>();
            userMetadata.put(Constants.CONSTANTS_SIGNAUTRE_KEY, file.signature());
            metadata.setUserMetadata(userMetadata);
            input = file.getInputStream();
            PutObjectRequest request = new PutObjectRequest(bucket, path, input, metadata);
            client.putObject(request);
        } catch (Exception e) {
            throw new StorageOperationException(e);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException ignored) {
                }
            }
        }

    }

    protected void initializeS3Settings(final Properties settings) {
        client = new AmazonS3Client(new AWSCredentials() {

            @Override
            public String getAWSSecretKey() {
                return settings.getProperty(AWS_S3_SECRET);
            }

            @Override
            public String getAWSAccessKeyId() {
                return settings.getProperty(AWS_S3_ACCESS_ID);
            }
        });

        bucket = settings.getProperty(AWS_S3_BUCKET);

        if (StringUtils.isBlank(bucket)) {
            throw new StorageOperationException(String.format("lost configuration property: %s", AWS_S3_BUCKET));
        }

        try {
            /* validate the bucket name */
            client.getBucketLocation(bucket);
        } catch (Exception e) {
            throw new StorageOperationException(e);
        }
    }

    @Override
    public boolean exists(String path) {
        try {
            return client.getObjectMetadata(bucket, path) != null;
        } catch (Exception e) {
            return false;
        }
    }

}
