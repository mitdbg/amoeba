package perf.benchmark;

import core.adapt.Query;
import core.adapt.spark.SparkQuery;
import core.common.globals.Globals;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by anil on 2/9/16.
 */
public class Shell {
    ConfUtils cfg;

    FileSystem fs;

    SparkQuery sq;

    public static void main(String[] args) {
        BenchmarkSettings.loadSettings(args);
        BenchmarkSettings.printSettings();

        Shell t = new Shell();
        t.loadSettings(args);
        t.setup();
        t.run();
    }

    public void loadSettings(String[] args) {

    }

    public void setup() {
        cfg = new ConfUtils(BenchmarkSettings.conf);
        fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
        sq = new SparkQuery(cfg);
    }

    public void run() {
        long start, end;
        BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.print(">>>> ");
            try {
                String queryString = console.readLine();
                Query q = new Query(queryString);
                Globals.loadTableInfo(q.getTable(), cfg.getHDFS_WORKING_DIR(), fs);

                start = System.currentTimeMillis();
                long result = runQuery(q);
                end = System.currentTimeMillis();
                System.out.println("RES: Time Taken: " + (end - start) +
                        "; Result: " + result);
            } catch (IOException e) {
                System.out.println("ERROR reading input");
            }
        }

        // TODO: Ideally we should close the stream.
    }

    public long runQuery(Query q) {
        return sq.createAdaptRDD(cfg.getHDFS_WORKING_DIR(), q).count();
    }
}
