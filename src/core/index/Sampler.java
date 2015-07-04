package core.index;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;

import core.index.build.InputReader;
import core.index.key.CartilageIndexKey;
import core.index.robusttree.RobustTreeHs;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;

/**
 * Created by qui on 5/21/15.
 */
public class Sampler {

    //public static String propertiesFile = "/Users/qui/Documents/mdindex/conf/cartilage.properties";
    public static String propertiesFile = "/home/mdindex/cartilage.properties";

    // Samples files in a directory and writes out the combined sample to HDFS.
    // Args:
    // -- sampleId: unique identifier for this sample
    // -- inputDirectory: directory of files to be sampled
    // -- prefix: prefix of files to sample
    // -- tpchSize: size of the full table, in MB
    // -- scaleFactor
    public static void main(String[] args) {
        int scaleFactor = Integer.parseInt(args[args.length-1]);
        int tpchSize = Integer.parseInt(args[args.length-2]); // should have as constants somewhere
        String prefix = args[args.length-3];
        String inputDirectory = args[args.length-4];
        String nodeId = args[args.length-5];

        String partitionDir = (new ConfUtils(propertiesFile)).getHDFS_HOMEDIR();

        int bucketSize = 64; // 64 mb
        int numBuckets = (scaleFactor * tpchSize) / bucketSize + 1;
        double samplingRate = ((double)numBuckets) / (60000 * scaleFactor);
        System.out.println("sampling rate: "+samplingRate);

        RobustTreeHs index = new RobustTreeHs(1);
        index.initBuild(numBuckets);
        CartilageIndexKey key = new CartilageIndexKey('|');
        InputReader r = new InputReader(index, key);
        long startTime = System.nanoTime();

        File[] files = new File(inputDirectory).listFiles();
        for (File f : files) {
            String inputFilename = f.getName();
            if (!inputFilename.startsWith(prefix)) {
                continue;
            }
            r.scanWithBlockSampling(f.getAbsolutePath(), samplingRate);
            r.firstPass = true;
        }
        System.out.println("SAMPLE: sampling time = " + ((System.nanoTime() - startTime) / 1E9));

        startTime = System.nanoTime();
        byte[] sampleBytes = index.serializeSample();

        ConfUtils conf = new ConfUtils(propertiesFile);
        FileSystem fs = HDFSUtils.getFS(conf.getHADOOP_HOME() + "/etc/hadoop/core-site.xml");
        OutputStream out = HDFSUtils.getHDFSOutputStream(fs, partitionDir + "/sample" + nodeId, (short) 1, 536870912);
        try {
            out.write(sampleBytes);
            out.flush();
            out.close();
            System.out.println("SAMPLE: sample writing time = " + ((System.nanoTime() - startTime) / 1E9));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
