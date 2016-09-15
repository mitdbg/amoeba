package perf.tools;

import core.adapt.Query;
import core.adapt.spark.SparkQuery;
import core.common.globals.Globals;
import core.common.globals.TableInfo;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;

/**
 * Utility used to run a workload of queries.
 * Loads queries and generates a log.
 */
public class RunWorkload {
    String tableName;

    String queriesFile;

    ConfUtils cfg;

    Query[] queries;

    SparkQuery sq;

    // 0 => SinglePred
    // 1 => MultiPred
    int mode = -1;

    public static void main(String[] args) {
        BenchmarkSettings.loadSettings(args);
        BenchmarkSettings.printSettings();

        RunWorkload t = new RunWorkload();
        t.loadSettings(args);
        t.setup();
        t.run();
    }

    public void loadSettings(String[] args) {
        int counter = 0;
        while (counter < args.length) {
            switch (args[counter]) {
                case "--tableName":
                    tableName = args[counter + 1];
                    counter += 2;
                    break;
                case "--queriesFile":
                    queriesFile = args[counter + 1];
                    counter += 2;
                    break;
                case "--mode":
                    mode = Integer.parseInt(args[counter + 1]);
                    counter += 2;
                    break;
                case "--c":
                    Globals.c = Double.parseDouble(args[counter + 1]);
                    counter += 2;
                    break;
                default:
                    // Something we don't use
                    counter += 2;
                    break;
            }
        }
    }

    public void setup() {
        cfg = new ConfUtils(BenchmarkSettings.conf);
        sq = new SparkQuery(cfg);

        List<Query> list = new ArrayList<>();

        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
        Globals.loadTableInfo(tableName, cfg.getHDFS_WORKING_DIR(), fs);
        TableInfo tableInfo = Globals.getTableInfo(tableName);

        File file = new File(queriesFile);
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(file));
            String text;

            while ((text = reader.readLine()) != null) {
                if (mode == 1) {
                    list.add(new Query(text));
                } else if (mode == 3) {
                    list.add(Query.getQueryFromFormattedString(tableInfo, text));
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
            }
        }

        System.out.println("Read " + list.size() + " queries from the file");

        queries = new Query[list.size()];
        queries = list.toArray(queries);
    }

    public long runQuery(Query q) {
        if (mode == 1 || mode == 3)
            return sq.createAdaptRDD(cfg.getHDFS_WORKING_DIR(), q).count();
        else
            return sq.createNoAdaptRDD(cfg.getHDFS_WORKING_DIR(), q).count();
    }

    public void run() {
        for (Query q: queries) {
                long start = System.currentTimeMillis();
                long result = runQuery(q);
                long end = System.currentTimeMillis();
                System.out.println("RES: Time Taken: " + (end - start) +
                        "; Result: " + result);
        }
    }
}
