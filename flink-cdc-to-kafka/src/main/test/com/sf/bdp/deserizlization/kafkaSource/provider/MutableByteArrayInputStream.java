package com.sf.bdp.deserizlization.kafkaSource.provider;

import java.io.ByteArrayInputStream;

public final class MutableByteArrayInputStream extends ByteArrayInputStream {

    public MutableByteArrayInputStream() {
        super(new byte[0]);
    }

    /**
     * Set buffer that can be read via the InputStream interface and reset the input stream. This
     * has the same effect as creating a new ByteArrayInputStream with a new buffer.
     *
     * @param buf the new buffer to read.
     */
    public void setBuffer(byte[] buf) {
        this.buf = buf;
        this.pos = 0;
        this.count = buf.length;
    }
}
