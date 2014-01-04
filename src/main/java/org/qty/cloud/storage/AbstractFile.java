package org.qty.cloud.storage;

public abstract class AbstractFile implements IFile {

    @Override
    final public boolean isTheSame(IFile other) {
        try {
            if (other == null) {
                return false;
            }

            if (length() != other.length()) {
                return false;
            }

            if (signature() == null) {
                return false;
            }
            if (other.signature() == null) {
                return false;
            }
            return signature().equals(other.signature());
        } catch (Exception e) {
            throw new StorageOperationException(e);
        }
    }

}
