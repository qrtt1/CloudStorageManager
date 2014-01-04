package org.qty.cloud.storage;

import java.io.InputStream;

public interface IFile {

    public String signature();

    public long length();

    public boolean isTheSame(IFile other);

    public InputStream getInputStream();

}
