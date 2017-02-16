package perf.tools;

import core.adapt.Query;
import core.common.globals.Globals;
import core.common.globals.TableInfo;
import core.simulator.Simulator;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;

/**
 * Utility used to run the simulator.
 * Loads queries and generates a log.
 */
public class RunSimulator {
    String tableName;

    String simName;

    String queriesFile;

    ConfUtils cfg;

    Query[] queries;

    // 0 => SinglePred
    // 1 => MultiPred
    int mode = -1;

    public static void main(String[] args) {
        BenchmarkSettings.loadSettings(args);
        BenchmarkSettings.printSettings();

        RunSimulator t = new RunSimulator();
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
                case "--simName":
                    simName = args[counter + 1];
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
                if (mode == 3) {
                    list.add(Query.getQueryFromFormattedString(tableInfo, text));
                } else {
                    list.add(new Query(text));
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

    public void run() {
        Simulator sim = new Simulator();
        sim.setUp(cfg, simName, tableName, queries);

        if (mode == 1 || mode == 3) {
            sim.runAdapt();
        } else if (mode == 2) {
            sim.runNoAdapt();
        } else {
            System.out.println("Unknown mode");
        }
    }
}
