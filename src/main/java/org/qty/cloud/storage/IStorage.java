package org.qty.cloud.storage;

/**
 * Provide the abstraction for Cloud Storage
 * 
 * @author qrtt1
 */
public interface IStorage {

    /**
     * Get the {@link IFile} reference under the path
     * 
     * @param path
     * @return
     */
    public IFile get(String path);

    /**
     * Put the content of {@link IFile} to the path
     * 
     * @param path
     * @param file
     */
    public void put(String path, IFile file);

    public boolean exists(String path);

}
