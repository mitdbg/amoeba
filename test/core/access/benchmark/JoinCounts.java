package core.access.benchmark;

import core.access.AccessMethod.PartitionSplit;
import core.access.Predicate;
import core.access.Predicate.PREDTYPE;
import core.access.Query;
import core.adapt.opt.Optimizer;
import core.index.Settings;
import core.utils.ConfUtils;
import core.utils.CuratorUtils;
import core.utils.SchemaUtils.TYPE;

import java.io.BufferedReader;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by qui on 7/6/15.
 */
public class JoinCounts {

    ConfUtils conf = new ConfUtils(Settings.cartilageConf);
    int scaleFactor = 1000;
    int[] numChunkValues = new int[]{1, 2, 4, 8, 16, 32, 50};
    Map<String, Map<Integer, Double>> bucketSizes = new HashMap<String, Map<Integer, Double>>();

    public void fillBucketSizes(String table) {
        Charset charset = Charset.forName("US-ASCII");
		Path file = FileSystems.getDefault().getPath("/Users/qui/Documents/exp/bucketsizes_" + table+".csv");
        bucketSizes.put(table, new HashMap<Integer, Double>());
        try {
            BufferedReader reader = Files.newBufferedReader(file, charset);
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split(",");
                bucketSizes.get(table).put(Integer.parseInt(tokens[0]), Double.parseDouble(tokens[1]));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void countJoinOverlapNaive(String big, String small) {
        fillBucketSizes(big);
        fillBucketSizes(small);

        long domainStart = 0;
        long range = scaleFactor * 6000000;

        for (int numChunks : numChunkValues) {
            double lineOverlap = 0;
            double partOverlap = 0;

            Map<Integer, Integer> lineCount = new HashMap<Integer, Integer>();
            Map<Integer, Integer> partCount = new HashMap<Integer, Integer>();
            for (int i = 0; i < numChunks; i++) {
                long start = domainStart + (range / numChunks) * i;
                long end = start + range / numChunks;

                Predicate part1 = new Predicate(0, TYPE.LONG, (long) start, PREDTYPE.GT);
                Predicate part2 = new Predicate(0, TYPE.LONG, (long) end, PREDTYPE.LEQ);

                Optimizer partOpt = new Optimizer(conf.getHDFS_HOMEDIR() + "/"+small, conf.getHADOOP_HOME());
                partOpt.loadIndex(conf.getZOOKEEPER_HOSTS());
                PartitionSplit[] partSplits = partOpt.buildAccessPlan(new Query.FilterQuery(new Predicate[]{part1, part2}));

                Predicate lineitem1 = new Predicate(1, TYPE.INT, (int) start, PREDTYPE.GT);
                Predicate lineitem2 = new Predicate(1, TYPE.INT, (int) end, PREDTYPE.LEQ);

                Optimizer lineOpt = new Optimizer(conf.getHDFS_HOMEDIR() + "/"+big, conf.getHADOOP_HOME());
                lineOpt.loadIndex(conf.getZOOKEEPER_HOSTS());
                PartitionSplit[] lineSplits = lineOpt.buildAccessPlan(new Query.FilterQuery(new Predicate[]{lineitem1, lineitem2}));

                for (PartitionSplit split : lineSplits) {
                    for (int j : split.getPartitions()) {
                        Double size = bucketSizes.get(big).get(j);
                        if (size != null) {
                            lineOverlap += size;
                        }
                    }
                }
                for (PartitionSplit split : partSplits) {
                    for (int j : split.getPartitions()) {
                        Double size = bucketSizes.get(small).get(j);
                        if (size != null) {
                            partOverlap += size;
                        }
                    }
                }
            }
            System.out.println(numChunks+","+lineOverlap+","+partOverlap);
        }
    }

    public void testOriginalSplit(String big, String small) {
        fillBucketSizes(small);
        fillBucketSizes(big);
        Optimizer partOpt = new Optimizer(conf.getHDFS_HOMEDIR() + "/" + small, conf.getHADOOP_HOME());
        partOpt.loadIndex(conf.getZOOKEEPER_HOSTS());

        Map<Integer, Integer> partCount = new HashMap<Integer, Integer>();
        double partOverlap = 0;
        double lineOverlap = 0;

        Charset charset = Charset.forName("US-ASCII");
        Path file = FileSystems.getDefault().getPath("/Users/qui/Documents/exp/lineitem_1_split_combo");
        try {
            BufferedReader reader = Files.newBufferedReader(file, charset);
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split(";");
                Predicate part1 = new Predicate(0, TYPE.LONG, Long.parseLong(tokens[1]), PREDTYPE.GT);
                Predicate part2 = new Predicate(0, TYPE.LONG, Long.parseLong(tokens[2]), PREDTYPE.LEQ);

                PartitionSplit[] partSplits = partOpt.buildAccessPlan(new Query.FilterQuery(new Predicate[]{part1, part2}));

                if (tokens[0].equals("")) {
                    continue;
                }
                double thisLine = 0;
                for (String partitionId : tokens[0].split(",")) {
                    Double size = bucketSizes.get(big).get(Integer.parseInt(partitionId));
                    if (size != null) {
                        thisLine += size;
                    }
                }

                double thisPart = 0;
                for (PartitionSplit split : partSplits) {
                    //System.out.println(Arrays.toString(split.getPartitions()));
                    for (int j : split.getPartitions()) {
                        Double size = bucketSizes.get(small).get(j);
                        if (size != null) {
                            thisPart += size;
                        }
                    }
                }

                partOverlap += thisPart;
                lineOverlap += thisLine;
                System.out.println("Range " + tokens[1] + "," + tokens[2]);
                System.out.println(small + ": " + thisPart);
                System.out.println(big + ": " + thisLine + "\n");
                //System.out.println(thisPart+thisLine+"\n");

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println(lineOverlap + "," + partOverlap);
        System.out.println(partOverlap);
    }

    public static void main(String[] args) {
        JoinCounts jc = new JoinCounts();
        //jc.countJoinOverlapNaive("lineitem", "orders");
        jc.testOriginalSplit("lineitem", "part");
    }
}
