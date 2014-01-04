package org.qty.cloud.storage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class StorageManager {

    static Log logger = LogFactory.getLog(StorageManager.class);

    /**
     * @param path
     *            The path under which the desired object is stored.
     * @param source
     *            The source storage where the object come from
     * @param destination
     *            The destination storage where the object will synchronize to
     * @throws StorageOperationException
     *             If any errors are encountered on the client while making the
     *             request or handling the response.
     */
    public void synchronize(String path, IStorage source, IStorage destination) {

        if (!source.exists(path)) {
            logger.warn("source file not found: " + path);
            return;
        }

        IFile sourceFile = source.get(path);

        if (destination.exists(path) && sourceFile.isTheSame(destination.get(path))) {
            logger.info("Found the same file in the storages, skip to synchronize the path: " + path);
            return;
        }

        destination.put(path, sourceFile);
    }

}
