package com.q.hello.nio;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

@Slf4j
public class NioTest {

    private static final String ROOT_FILE_PATH;

    static {
        ROOT_FILE_PATH = Objects.requireNonNull(NioTest.class.getClassLoader().getResource(".")).getFile();
    }

    @Test
    public void fileNioWrite() throws Exception {
        try (RandomAccessFile file = new RandomAccessFile(
            new File(ROOT_FILE_PATH, "data-" + System.currentTimeMillis() + ".txt"), "rw")) {
            FileChannel channel = file.getChannel();

            ByteBuffer buf = ByteBuffer.allocate(1024);
            buf.put("hello, world\n".getBytes(StandardCharsets.UTF_8));
            buf.flip();
            channel.write(buf);
        }
    }

    @Test
    public void fileNio() throws Exception {
        try (RandomAccessFile file = new RandomAccessFile(new File(ROOT_FILE_PATH, "data.txt"), "rw")) {
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
