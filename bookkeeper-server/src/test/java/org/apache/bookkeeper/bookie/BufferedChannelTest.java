package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

import static org.junit.jupiter.api.Assertions.*;

class BufferedChannelTest {

    private Path tempFile;
    private FileChannel fc;
    private BufferedChannel channel;
    private final ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;

    @BeforeEach
    void setUp() throws IOException {
        tempFile = Files.createTempFile("bufchan", ".tmp");
        fc = FileChannel.open(tempFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
        channel = new BufferedChannel(allocator, fc, 256);
    }

    @AfterEach
    void tearDown() throws IOException {
        channel.close();
        fc.close();
        Files.deleteIfExists(tempFile);
    }

    @Test
    void testWriteFlushAndReadBack() throws IOException {
        // Scrivo "ciao" nel buffer
        ByteBuf src = Unpooled.wrappedBuffer("ciao".getBytes(StandardCharsets.UTF_8));
        channel.write(src);
        channel.flush();

        // Preparo un ByteBuf di destinazione e leggo dal canale
        ByteBuf dest = allocator.directBuffer(4);
        int read = channel.read(dest, 0, 4);
        assertEquals(4, read, "Dovrebbe leggere esattamente 4 byte");

        byte[] result = new byte[4];
        dest.readBytes(result);
        assertEquals("ciao", new String(result, StandardCharsets.UTF_8));
    }

    @Test
    void testCloseIsIdempotent() {
        assertDoesNotThrow(() -> {
            channel.close();
            channel.close();  // chiudere una seconda volta non deve fallire
        });
    }
}
