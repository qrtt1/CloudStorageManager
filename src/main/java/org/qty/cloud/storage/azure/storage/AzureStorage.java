package org.qty.cloud.storage.azure.storage;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Properties;

import org.qty.cloud.storage.Constants;
import org.qty.cloud.storage.IFile;
import org.qty.cloud.storage.IStorage;
import org.qty.cloud.storage.StorageOperationException;

import com.microsoft.windowsazure.services.blob.client.CloudBlob;
import com.microsoft.windowsazure.services.blob.client.CloudBlobClient;
import com.microsoft.windowsazure.services.blob.client.CloudBlobContainer;
import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.core.storage.StorageException;

public class AzureStorage implements IStorage {

    public static final String AZURE_STORAGE_ACCESS_ID = "azure.storage.accessId";
    public static final String AZURE_STORAGE_SECRET = "azure.storage.secret";
    public static final String AZURE_STORAGE_CONTAINER = "azure.storage.container";

    CloudBlobClient serviceClient;
    CloudBlobContainer container;

    public AzureStorage(Properties properties) {
        try {
            initializeAzureStorageSettings(properties);
        } catch (Exception e) {
            throw new StorageOperationException(e);
        }
    }

    @Override
    public IFile get(String path) {
        try {
            return new AzureStorageFile(this, path);
        } catch (Exception e) {
            throw new StorageOperationException(e);
        }
    }

    @Override
    public void put(String path, IFile file) {
        InputStream input = null;
        try {
            input = file.getInputStream();
            CloudBlob blob = container.getBlockBlobReference(path);
            blob.upload(input, file.length());
            blob.getMetadata().put(Constants.CONSTANTS_SIGNAUTRE_KEY, file.signature());
            blob.uploadMetadata();
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

    protected void initializeAzureStorageSettings(Properties properties) throws URISyntaxException,
            InvalidKeyException, StorageException {
        String info = String.format("DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s",
                properties.getProperty(AZURE_STORAGE_ACCESS_ID), properties.getProperty(AZURE_STORAGE_SECRET));
        serviceClient = CloudStorageAccount.parse(info).createCloudBlobClient();
        container = serviceClient.getContainerReference(properties.getProperty(AZURE_STORAGE_CONTAINER));
    }

    @Override
    public boolean exists(String path) {
        try {
            return container.getBlockBlobReference(path).exists();
        } catch (Exception e) {
            return false;
        }
    }
}
