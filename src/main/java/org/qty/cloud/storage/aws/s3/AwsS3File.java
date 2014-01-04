package org.qty.cloud.storage.aws.s3;

import java.io.InputStream;

import org.qty.cloud.storage.AbstractFile;
import org.qty.cloud.storage.Constants;
import org.qty.cloud.storage.IFile;
import org.qty.cloud.storage.StorageOperationException;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;

public class AwsS3File extends AbstractFile implements IFile {

    private AwsS3Storage awsS3Storage;
    private String path;
    private ObjectMetadata metadata;

    public AwsS3File(AwsS3Storage awsS3Storage, String path) {
        this.awsS3Storage = awsS3Storage;
        this.path = path;

        metadata = awsS3Storage.client.getObjectMetadata(awsS3Storage.bucket, path);
    }

    @Override
    public String signature() {
        try {
            String sig = metadata.getUserMetadata().get(Constants.CONSTANTS_SIGNAUTRE_KEY);
            if (sig != null) {
                return sig;
            }
            return metadata.getETag();
        } catch (Exception e) {
            throw new StorageOperationException(e);
        }
    }

    @Override
    public long length() {
        try {
            return metadata.getContentLength();
        } catch (Exception e) {
            throw new StorageOperationException(e);
        }
    }

    @Override
    public InputStream getInputStream() {
        try {
            return awsS3Storage.client.getObject(new GetObjectRequest(awsS3Storage.bucket, path)).getObjectContent();
        } catch (Exception e) {
            throw new StorageOperationException(e);
        }
    }

}
