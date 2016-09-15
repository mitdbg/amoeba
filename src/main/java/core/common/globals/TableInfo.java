package core.common.globals;

import core.utils.HDFSUtils;
import core.utils.TypeUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by anil on 12/11/15.
 */
public class TableInfo {
    // Name of table.
    public String tableName;

    // Total number of tuples in dataset.
    public double numTuples;

    // TPC-H generated files use '|'. CSV uses ','.
    public char delimiter;

    // Schema of data set.
    public Schema schema;

    public TableInfo(String tableName) {
        this(tableName, 0, '|', null);
    }

    public TableInfo(String tableName, double numTuples, char delimiter, Schema schema) {
        this.tableName = tableName;
        this.numTuples = numTuples;
        this.delimiter = delimiter;
        this.schema = schema;
    }

    public TypeUtils.TYPE[] getTypeArray() {
        return schema.getTypeArray();
    }

    public void save(String hdfsWorkingDir, short replication, FileSystem fs) {
        String saveContent = "TOTAL_NUM_TUPLES: " + numTuples + "\n" +
                "DELIMITER: " + delimiter + "\n" +
                "SCHEMA: " + schema.toString() + "\n";
        byte[] saveContentBytes = saveContent.getBytes();
        String path = hdfsWorkingDir + "/" + tableName + "/info";
        HDFSUtils.writeFile(fs, path, replication,
                saveContentBytes, 0, saveContentBytes.length, false);
    }

    public void load(String hdfsWorkingDir, FileSystem fs) {
        String path = hdfsWorkingDir + "/" + tableName + "/info";
        byte[] fileContent = HDFSUtils.readFile(fs, path);
        String content = new String(fileContent);

        String[] settings = content.split("\n");
        assert settings.length == 3;

        for (int i = 0; i < settings.length; i++) {
            String setting = settings[i];
            String[] parts = setting.split(":");
            switch (parts[0].trim()) {
                case "TOTAL_NUM_TUPLES":
                    numTuples = Double.parseDouble(parts[1].trim());
                    break;
                case "DELIMITER":
                    delimiter = parts[1].trim().charAt(0);
                    break;
                case "SCHEMA":
                    schema = Schema.createSchema(parts[1].trim());
                    break;
                default:
                    System.out.println("Unknown setting found: " + parts[0].trim());
            }
        }
    }

}
