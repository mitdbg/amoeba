package core.utils;

import com.google.common.io.ByteStreams;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;

/**
 * The goal of this class is to reduce large byte array allocations.
 *
 * @author alekh
 */
public class BufferManager {

    public final static int DEFAULT_BUFFER_SIZE = 1024 * 1024 * 50;
    public final static double resizeFraction = 0.5; // increase byte size by
    private byte[] byteArr;
    // 50% in case it
    // overshoots

    public BufferManager() {
        byteArr = new byte[DEFAULT_BUFFER_SIZE];
    }

    public BufferManager(int initialSize) {
        byteArr = new byte[initialSize];
    }

    public static void main(String[] args) {
        byte[] b = "test124".getBytes();
        ByteBuffer bb = ByteBuffer.wrap(b, 2, 2);

        System.out.println(bb.position());
        System.out.println(bb.limit());
        System.out.println(bb.remaining());

        System.out.println((char) bb.get());

        bb.clear();

        System.out.println(bb.position());
        System.out.println(bb.limit());
        System.out.println(bb.remaining());

    }

    private int getResize() {
        return (int) ((1 + resizeFraction) * byteArr.length);
    }

    // public ByteBuffer load(InputStream in, int size){
    // try {
    // while(size > byteArr.length)
    // byteArr = new byte[getResize()];
    // ByteStreams.readFully(in, byteArr, 0, size);
    //
    // if(size > 0)
    // ByteStreams.readFully(in, byteArr, 0, size);
    // else
    // ByteStreams.readFully(in, byteArr);
    //
    // return ByteBuffer.wrap(byteArr, 0, size);
    //
    // } catch (IOException e) {
    // e.printStackTrace();
    // throw new
    // RuntimeException("Failed to read "+size+" bytes from the input stream!");
    // }
    // }

    public ByteBuffer load(InputStream in) {
        try {
            int offset = 0;
            int readLength = ByteStreams.read(in, byteArr, offset,
                    byteArr.length - offset);
            while (readLength >= byteArr.length - offset) {
                offset = byteArr.length;
                byteArr = Arrays.copyOf(byteArr, getResize());
                readLength = ByteStreams.read(in, byteArr, offset,
                        byteArr.length - offset);
            }
            return ByteBuffer.wrap(byteArr, 0, offset + readLength);

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(
                    "Failed to read bytes from the input stream!");
        }
    }

    public ByteBuffer loadEmpty() {
        return ByteBuffer.wrap(byteArr);
    }

    public void store(OutputStream out, ByteBuffer bb) {
        store(Channels.newChannel(out), bb);
    }

    public void store(WritableByteChannel channel, ByteBuffer bb) {
        try {
            channel.write(bb);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(
                    "Failed to write bytes to the input stream!");
        }
    }
}
