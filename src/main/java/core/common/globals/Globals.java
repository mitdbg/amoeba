package core.common.globals;

import org.apache.hadoop.fs.FileSystem;

import java.util.HashMap;
import java.util.Map;

/**
 * Stores global information.
 * Currently stores the table information and zookeeper hosts
 *
 * @author anil
 */
public class Globals {
    // Re-partition cost multiplier.
    static public double c = 4;

    // Query window size.
    static public final int window_size = 20;

    static Map<String, TableInfo> tableInfos = new HashMap<String, TableInfo>();

    public static TableInfo getTableInfo(String tableName) {
        return tableInfos.get(tableName);
    }

    public static void addTableInfo(TableInfo tableInfo) {
        tableInfos.put(tableInfo.tableName, tableInfo);
    }

    public static void saveTableInfo(String tableName, String hdfsWorkingDir, short replication, FileSystem fs) {
        TableInfo tableInfo = tableInfos.get(tableName);
        tableInfo.save(hdfsWorkingDir, replication, fs);
    }

    public static void loadTableInfo(String tableName, String hdfsWorkingDir, FileSystem fs) {
        if (tableInfos.get(tableName) == null) {
            TableInfo tableInfo = new TableInfo(tableName);
            tableInfo.load(hdfsWorkingDir, fs);
            tableInfos.put(tableName, tableInfo);
        }
    }
}
