package org.qty.cloud.storage.azure.storage;

import java.io.InputStream;
import java.net.URISyntaxException;

import org.qty.cloud.storage.AbstractFile;
import org.qty.cloud.storage.Constants;
import org.qty.cloud.storage.IFile;
import org.qty.cloud.storage.StorageOperationException;

import com.microsoft.windowsazure.services.blob.client.CloudBlob;
import com.microsoft.windowsazure.services.blob.client.CloudBlockBlob;
import com.microsoft.windowsazure.services.core.storage.StorageException;

public class AzureStorageFile extends AbstractFile implements IFile {

    private AzureStorage azureStorage;
    private String path;

    public AzureStorageFile(AzureStorage azureStorage, String path) {
        this.azureStorage = azureStorage;
        this.path = path;

    }

    @Override
    public String signature() {
        try {
            CloudBlob blob = getBlob();
            fetchAttributes(blob);

            String sig = blob.getMetadata().get(Constants.CONSTANTS_SIGNAUTRE_KEY);
            if (sig != null) {
                return sig;
            }

            return blob.getProperties().getEtag();

        } catch (Exception e) {
            throw new StorageOperationException(e);
        }

    }

    @Override
    public long length() {
        try {
            CloudBlob blob = getBlob();
            fetchAttributes(blob);
            return blob.getProperties().getLength();
        } catch (Exception e) {
            throw new StorageOperationException(e);
        }
    }

    protected CloudBlockBlob getBlob() throws URISyntaxException, StorageException {
        return azureStorage.container.getBlockBlobReference(path);
    }

    @Override
    public InputStream getInputStream() {
        try {
            CloudBlob blob = getBlob();
            return blob.openInputStream();
        } catch (Exception e) {
            throw new StorageOperationException(e);
        }
    }

    private synchronized void fetchAttributes(CloudBlob blob) {
        if (blob.getMetadata().get(Constants.CONSTANTS_SIGNAUTRE_KEY) != null || blob.getProperties().getEtag() != null) {
            return;
        }
        try {
            blob.downloadAttributes();
        } catch (Exception e) {
            throw new StorageOperationException(e);
        }
    }

}
