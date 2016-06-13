package perf.benchmark;

import core.common.globals.Globals;
import core.common.globals.Schema;
import core.common.globals.TableInfo;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;

/**
 * Created by anil on 12/11/15.
 */
public class CreateTableInfo {
    double numTuples = -1;
    char delimiter = '9'; // Assuming '9' is never a delimiter.
    Schema schema = null;
    String tableName = "";

    /**
     * Used to create a table.
     */
    public static void main(String[] args) {
        BenchmarkSettings.loadSettings(args);
        CreateTableInfo cti = new CreateTableInfo();
        cti.loadSettings(args);
        cti.createTableInfo();
    }

    public void loadSettings(String[] args) {
        int counter = 0;
        while (counter < args.length) {
            switch (args[counter]) {
                case "--tableName":
                    tableName = args[counter + 1];
                    counter += 2;
                    break;
                case "--numTuples":
                    numTuples = Double.parseDouble(args[counter + 1]);
                    counter += 2;
                    break;
                case "--delimiter":
                    delimiter = args[counter + 1].trim().charAt(0);
                    counter += 2;
                    break;
                case "--schema":
                    String schemaString = args[counter + 1];
                    schema = Schema.createSchema(schemaString);
                    counter += 2;
                    break;
                default:
                    // Something we don't use
                    counter += 2;
                    break;
            }
        }
    }

    public void createTableInfo() {
        TableInfo tableInfo = new TableInfo(tableName, numTuples, delimiter, schema);
        Globals.addTableInfo(tableInfo);
        ConfUtils cfg = new ConfUtils(BenchmarkSettings.conf);
        Globals.saveTableInfo(tableName, cfg.getHDFS_WORKING_DIR(),
                cfg.getHDFS_REPLICATION_FACTOR(),
                HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME()));
    }
}
