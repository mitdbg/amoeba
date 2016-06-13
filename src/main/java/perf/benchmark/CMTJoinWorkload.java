package perf.benchmark;

import core.adapt.JoinQuery;
import core.adapt.Predicate;
import core.adapt.spark.join.SparkJoinQuery;
import core.common.globals.Schema;
import core.common.globals.TableInfo;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import core.utils.RangePartitionerUtils;
import core.utils.TypeUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;


public class CMTJoinWorkload {

    private ConfUtils cfg;

    private Schema schemaMH, schemaMHL, schemaSF;
    private String stringMH, stringMHL, stringSF;
    private int sizeMH, sizeMHL, sizeSF;
    private String MH = "mh", MHL = "mhl", SF = "sf";
    private TableInfo tableMH, tableMHL, tableSF;
    private ArrayList<Long> sf_id_keys;

    private Predicate[] EmptyPredicates = {};


    private int method;

    private int numQueries;

    private Random rand;

    public static void main(String[] args) {

        BenchmarkSettings.loadSettings(args);
        BenchmarkSettings.printSettings();

        CMTJoinWorkload t = new CMTJoinWorkload();
        t.loadSettings(args);
        t.setUp();

        switch (t.method) {
            case 1:
                t.runWorkload();
                break;
            default:
                break;
        }
    }

    public void setUp() {
        cfg = new ConfUtils(BenchmarkSettings.conf);
        rand = new Random();

        // Making things more deterministic.
        rand.setSeed(0);

        tableMH = new TableInfo(MH, 0, '|', schemaMH);
        tableMHL = new TableInfo(MHL, 0, '|', schemaMHL);
        tableSF = new TableInfo(SF, 0, '|', schemaSF);

        String workingDir = cfg.getHDFS_WORKING_DIR();

        sf_id_keys = RangePartitionerUtils.getKeys(cfg, tableSF, workingDir + "/" + SF + "/sample", schemaSF.getAttributeId("sf_id"));
    }

    public void garbageCollect() {
        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());

        tableMH.gc(cfg.getHDFS_WORKING_DIR(), fs);
        tableMHL.gc(cfg.getHDFS_WORKING_DIR(), fs);
        tableSF.gc(cfg.getHDFS_WORKING_DIR(), fs);
    }

    public void loadSettings(String[] args) {
        int counter = 0;
        while (counter < args.length) {
            switch (args[counter]) {
                case "--schemaMH":
                    stringMH = args[counter + 1];
                    schemaMH = Schema.createSchema(stringMH);
                    counter += 2;
                    break;
                case "--schemaMHL":
                    stringMHL = args[counter + 1];
                    schemaMHL = Schema.createSchema(stringMHL);
                    counter += 2;
                    break;
                case "--schemaSF":
                    stringSF = args[counter + 1];
                    schemaSF = Schema.createSchema(stringSF);
                    counter += 2;
                    break;
                case "--sizeMH":
                    sizeMH = Integer.parseInt(args[counter + 1]);
                    counter += 2;
                    break;
                case "--sizeMHL":
                    sizeMHL = Integer.parseInt(args[counter + 1]);
                    counter += 2;
                    break;
                case "--sizeSF":
                    sizeSF = Integer.parseInt(args[counter + 1]);
                    counter += 2;
                    break;
                case "--method":
                    method = Integer.parseInt(args[counter + 1]);
                    counter += 2;
                    break;
                case "--numQueries":
                    numQueries = Integer.parseInt(args[counter + 1]);
                    counter += 2;
                    break;
                default:
                    // Something we don't use
                    counter += 2;
                    break;
            }
        }
    }

    public void cleanup(String path) {
        FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
        try {
            fs.delete(new Path(path), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void postProcessing(String path, String tableName, Schema schema) {

        /* rename part-0000i to i and create an info file*/

        try {
            FileSystem fs = HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME());
            String dest = path + "/data";

            // delete _SUCCESS

            fs.delete(new Path(dest + "/_SUCCESS"), false);
            FileStatus[] fileStatus = fs.listStatus(new Path(dest));

            for (int i = 0; i < fileStatus.length; i++) {
                String oldPath = fileStatus[i].getPath().toString();
                String baseName = FilenameUtils.getBaseName(oldPath);
                String dir = oldPath.substring(0, oldPath.length() - baseName.length());
                String newPath = dir + Integer.parseInt(baseName.substring(baseName.indexOf('-') + 1));

                fs.rename(new Path(oldPath), new Path(newPath));
            }


            /*  write out a fake (TOTAL_NUM_TUPLES is 0, delimiter is set to '|') info to make HDFSPartition Happy*/

            TableInfo tableInfo = new TableInfo(tableName, 0, ';', schema);
            tableInfo.save(cfg.getHDFS_WORKING_DIR(), cfg.getHDFS_REPLICATION_FACTOR(), fs);


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Predicate getPredicate(Schema schema, String pred) {
        String[] parts = pred.split(" ");
        int attrId = schema.getAttributeId(parts[0].trim());

        if (attrId == -1) {
            throw new RuntimeException("Unknown attr: " + parts[0].trim());
        }

        TypeUtils.TYPE attrType = schema.getType(attrId);
        Object value = TypeUtils.deserializeValue(attrType, parts[2].trim().replaceAll("'", ""));
        String predTypeStr = parts[1].trim();
        Predicate.PREDTYPE predType;
        switch (predTypeStr) {
            case ">":
                predType = Predicate.PREDTYPE.GT;
                break;
            case ">=":
                predType = Predicate.PREDTYPE.GEQ;
                break;
            case "<":
                predType = Predicate.PREDTYPE.LT;
                break;
            case "<=":
                predType = Predicate.PREDTYPE.LEQ;
                break;
            case "=":
                predType = Predicate.PREDTYPE.EQ;
                break;
            default:
                throw new RuntimeException("Unknown predType " + predTypeStr);
        }

        Predicate p = new Predicate(attrId, attrType, value, predType);
        return p;
    }

    public ArrayList<ArrayList<JoinQuery>> generateWorkload() {
        byte[] stringBytes = HDFSUtils.readFile(
                HDFSUtils.getFSByHadoopHome(cfg.getHADOOP_HOME()),
                "/user/yilu/cmt100000000/cmt_queries.log");

        String queriesString = new String(stringBytes);
        String[] queries = queriesString.split("\n");
        ArrayList<ArrayList<JoinQuery>> ret = new ArrayList<ArrayList<JoinQuery>>();
        for (int i = 0; i < queries.length; i++) {
            String query = queries[i];
            String[] predicates = query.split(";");
            ArrayList<Predicate> mhPreds = new ArrayList<Predicate>();
            ArrayList<Predicate> sfPreds = new ArrayList<Predicate>();

            ArrayList<JoinQuery> q = new ArrayList<JoinQuery>();

            for (int j = 0; j < predicates.length; j++) {
                if (predicates[j].startsWith(MH)) {
                    Predicate p = getPredicate(schemaMH, predicates[j]);
                    mhPreds.add(p);
                } else {
                    Predicate p = getPredicate(schemaSF, predicates[j]);
                    sfPreds.add(p);
                }
            }

            Predicate[] mhArray = mhPreds.toArray(new Predicate[mhPreds.size()]);
            Predicate[] sfArray = sfPreds.toArray(new Predicate[sfPreds.size()]);

            JoinQuery q_mh = new JoinQuery(MH, schemaMH.getAttributeId("mh_id"), mhArray);
            JoinQuery q_sf = new JoinQuery(SF, schemaSF.getAttributeId("sf_id"), sfArray);

            q.add(q_mh);
            q.add(q_sf);

            ret.add(q);
        }

        return ret;
    }

    // sf ⋈ (mhl ⋈ mh)
    public void runWorkload() {


        ArrayList<ArrayList<JoinQuery>> queries = generateWorkload();
        SparkJoinQuery sq = new SparkJoinQuery(cfg);
        int iters = 0;

        for (ArrayList<JoinQuery> q : queries) {

            JoinQuery q_mh = q.get(0);
            JoinQuery q_sf = q.get(1);
            JoinQuery q_mhl = new JoinQuery(MHL, schemaMHL.getAttributeId("mhl_mapmatch_history_id"), EmptyPredicates);

            if (++iters == 5) {
                q_mh.setForceRepartition(true);
                q_sf.setForceRepartition(true);
                q_mhl.setForceRepartition(true);
            }

            System.out.println("INFO: Query_MH:" + q_mh.toString());
            System.out.println("INFO: Query_sf:" + q_sf.toString());


            long start = System.currentTimeMillis();

            String stringMH_join_MHL = stringMH + ", " + stringMHL;
            Schema schemaMH_join_MHL = Schema.createSchema(stringMH_join_MHL);

            JavaPairRDD<LongWritable, Text> mh_join_mhl_rdd = sq.createJoinRDD(MH, q_mh, "NULL", MHL, q_mhl, "NULL", schemaMH_join_MHL.getAttributeId("mhl_dataset_id"));
            JavaPairRDD<LongWritable, Text> sf_rdd = sq.createScanRDD(SF, q_sf);
            JavaPairRDD<LongWritable, Tuple2<Text, Text>> rdd = mh_join_mhl_rdd.join(sf_rdd);
            long result = rdd.count();

            System.out.println("RES: Time Taken: " + (System.currentTimeMillis() - start) + "; Result: " + result);

            garbageCollect();
        }
    }
}
