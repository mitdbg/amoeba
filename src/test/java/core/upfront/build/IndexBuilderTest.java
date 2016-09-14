package core.upfront.build;

import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import junit.framework.TestCase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import perf.tools.BenchmarkSettings;
import perf.tools.CreateTableInfo;
import perf.tools.RunIndexBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Created by anil on 12/18/15.
 */
public class IndexBuilderTest extends TestCase {
    ConfUtils cfg;
    FileSystem fs;

    public void createTableInfo(String[] args) {
        CreateTableInfo cti = new CreateTableInfo();
        cti.loadSettings(args);
        cti.createTableInfo();
    }

    public void testEndToEnd() {
        String HOME_DIR = "/Users/anil/Dev/repos/mdindex";
        Integer NUM_BUCKETS = 16;
        Integer NUM_TUPLES = 30329;
        String TABLE_NAME = "tpch";

        String[] args = new String[]{
                "--tableName", TABLE_NAME,
                "--numTuples", NUM_TUPLES.toString(),
                "--delimiter", "|",
                "--schema", "l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string,  l_linestatus string, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct string, l_shipmode string, o_orderstatus string, o_totalprice double, o_orderdate date, o_orderpriority string, o_clerk string, o_shippriority int, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double, s_name string, s_address string, s_phone string, s_acctbal double, s_nation string, s_region string, c_name string, c_address string, c_phone string, c_acctbal double, c_mktsegment string, c_nation string, c_region string",
                "--inputsDir", HOME_DIR + "/data/tpchd/",
                "--samplingRate", "0.1",
                "--numBuckets", NUM_BUCKETS.toString(),
                "--conf", HOME_DIR + "/conf/test.properties"
        };
        BenchmarkSettings.loadSettings(args);

        cfg = new ConfUtils(BenchmarkSettings.conf);
        fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());

        HDFSUtils.deleteFile(fs, cfg.getHDFS_WORKING_DIR(), true);
        HDFSUtils.safeCreateDirectory(fs, cfg.getHDFS_WORKING_DIR());

        createTableInfo(args);

        RunIndexBuilder t = new RunIndexBuilder();
        t.loadSettings(args);
        t.setUp();

        t.createSamples();
        t.buildRobustTreeFromSamples();
        t.writePartitionsFromIndex();

        // Make sure the necessary files have been created.
        String basePath = cfg.getHDFS_WORKING_DIR();
        try {
            assertTrue(fs.exists(new Path(basePath + "/tpch/info")));
            assertTrue(fs.exists(new Path(basePath + "/tpch/sample")));
            assertTrue(fs.exists(new Path(basePath + "/tpch/index")));
            for (int i=0; i<16; i++) {
                assertTrue(fs.exists(new Path(basePath + "/tpch/data/" + i)));
            }
        } catch(IOException e) {
            fail("fs.exists call failed");
        }

        // Make sure no tuples were lost.
        int total = 0;
        for (int i=0; i<16; i++) {
            List<String> lines = HDFSUtils.readHDFSLines(fs,
                    cfg.getHDFS_WORKING_DIR() + "/" + TABLE_NAME + "/data/" + i);
            total += lines.size();
        }
        assertTrue(total == NUM_TUPLES);
    }
}
