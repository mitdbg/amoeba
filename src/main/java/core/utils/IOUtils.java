package core.utils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.List;

public class IOUtils {

    private static final int READ_BUFFER_SIZE = 20 * 1024 * 1024;
    private static final int WRITE_BUFFER_SIZE = 2 * 1024 * 1024;
    private static final int BYTE_ARRAY_SIZE = 32 * 1024 * 1024;

    public static BufferedReader openBufferedFile(String filename) {
        try {
            return new BufferedReader(new FileReader(filename),
                    READ_BUFFER_SIZE);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File to parse not found. "
                    + e.getMessage());
        }
    }

    public static void closeBufferedFile(BufferedReader reader) {
        try {
            if (reader != null)
                reader.close();
        } catch (IOException e) {
            throw new RuntimeException("Cannot close file. " + e.getMessage());
        }
    }

    @SuppressWarnings("resource")
    public static FileChannel openFileChannel(String filename) {
        FileInputStream f;
        try {
            f = new FileInputStream(filename);
            return f.getChannel();
        } catch (FileNotFoundException e) {
            throw new IllegalAccessError("Unable to access file " + filename);
        }
    }

    public static void closeFileChannel(FileChannel ch) {
        try {
            if (ch != null)
                ch.close();
        } catch (IOException e) {
            throw new RuntimeException("Cannot close file. " + e.getMessage());
        }
    }

    public static byte[] readByteArray(String fileName) {
        try {
            return FileUtils.readFileToByteArray(new File(fileName));
        } catch (IOException e) {
            throw new RuntimeException("Cannot read file" + fileName
                    + " into byte array");
        }
    }

    public static InputStream getFileInputStream(String filename) {
        try {
            return new FileInputStream(filename);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Could not open the file:" + filename);
        }
    }

    public static OutputStream getFileOutputStream(String filename) {
        try {
            return new BufferedOutputStream(new FileOutputStream(filename),
                    WRITE_BUFFER_SIZE);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Could not open the file:" + filename);
        }
    }

    public static OutputStream getArrayOutputStream() {
        return new ByteArrayOutputStream(BYTE_ARRAY_SIZE);
    }

    public static void writeOutputStream(OutputStream writer, byte[] bytes) {
        try {
            writer.write(bytes);
        } catch (IOException e) {
            throw new RuntimeException("Cannot write to file. "
                    + e.getMessage());
        }
    }

    public static void writeOutputStream(OutputStream writer, List<String> lines) {
        try {
            for (String line : lines) {
                writer.write(line.getBytes());
                writer.write('\n');
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot write to file. "
                    + e.getMessage());
        }
    }

    public static void closeOutputStream(OutputStream writer) {
        try {
            writer.flush();
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException("Cannot close file. " + e.getMessage());
        }
    }

    public static void closeInputStream(InputStream in) {
        try {
            in.close();
        } catch (IOException e) {
            throw new RuntimeException("Cannot close file. " + e.getMessage());
        }
    }

    public static void writeByteArray(String filename, byte[] array,
                                      int offset, int length, boolean append) {
        try {
            FileOutputStream fout = new FileOutputStream(filename, append);
            fout.write(array, offset, length);
            fout.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeByteArray(String filename, byte[] array) {
        try {
            FileUtils.writeByteArrayToFile(new File(filename), array);
        } catch (IOException e) {
            throw new RuntimeException("Cannot write byte array to file!");
        }
    }

    public static void appendByteArray(String filename, byte[] array) {
        try {
            FileUtils.writeByteArrayToFile(new File(filename), array, true);
        } catch (IOException e) {
            throw new RuntimeException("Cannot write byte array to file!");
        }
    }

    public static void rename(String from, String to) {
        try {
            FileUtils.moveFile(new File(from), new File(to));
        } catch (IOException e) {
            throw new RuntimeException("cannot rename the file: " + from);
        }
    }

    public static boolean checkFileExists(String filename) {
        File dir = new File(FilenameUtils.getPathNoEndSeparator(filename));
        File file = new File(dir, FilenameUtils.getName(filename));
        try {
            return FileUtils.directoryContains(dir, file);
        } catch (IOException e) {
            throw new RuntimeException("cannot detect the file: " + filename);
        }
    }

    public static List<String> readLines(String filename) {
        try {
            return FileUtils.readLines(new File(filename));
        } catch (IOException e) {
            throw new RuntimeException("could not read the file: " + filename);
        }
    }

    public static void writeLines(String filename, Collection<?> lines) {
        try {
            FileUtils.writeLines(new File(filename), lines);
        } catch (IOException e) {
            throw new RuntimeException("could not write to file: " + filename);
        }
    }
}
