package com.q.hello.nio;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;

@Slf4j
public class NioTest {

    @Test
    public void fileNio() throws Exception {
        String filePath = Objects.requireNonNull(getClass().getClassLoader().getResource("data.txt")).getFile();
        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            FileChannel channel = file.getChannel();
            ByteBuffer buf = ByteBuffer.allocate(1024);
            int bytesRead = channel.read(buf);
            while (bytesRead > 0) {
                log.info("bytes read: " + bytesRead);

                // 开始读
                buf.flip();
                while (buf.hasRemaining()) {
                    log.info("" + (char) buf.get());
                }
                buf.clear();
                bytesRead = channel.read(buf);
            }
        }
    }

}
