package core.index;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import core.index.key.CartilageIndexKeySet;
import core.index.robusttree.RobustTreeHs;
import core.utils.ConfUtils;
import core.utils.HDFSUtils;

/**
 * Created by qui on 5/22/15.
 */
public class Builder {

    static int bucketSize = 64; // 64 mb
    static String propertiesFile = "/Users/qui/Documents/mdindex/conf/cartilage.properties";
    //static String propertiesFile = "/home/mdindex/cartilage.properties";

    // Script to build an index from a collection of sample files.
    // Args:
    // -- tpchSize: size of the full table, in MB
    // -- samplingRate: how much to sample from the samples (usually 1, unless samples are large)
    public static void main(String[] args) {
        double samplingRate = Double.parseDouble(args[args.length-1]);
        int tpchSize = Integer.parseInt(args[args.length - 2]);
        String hdfsDir = new ConfUtils(propertiesFile).getHDFS_HOMEDIR();

        int numBuckets = tpchSize / bucketSize + 1;

        RobustTreeHs index = new RobustTreeHs(samplingRate);
        //KDMedianTree index = new KDMedianTree(samplingRate);
        index.initBuild(numBuckets);

        ConfUtils conf = new ConfUtils(propertiesFile);
        FileSystem fs = HDFSUtils.getFS(conf.getHADOOP_HOME() + "/etc/hadoop/core-site.xml");
        long startTime = System.nanoTime();

        // read all the sample files and put them into the sample key set
        CartilageIndexKeySet sample = new CartilageIndexKeySet();
        try {
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(hdfsDir), false);
            while (files.hasNext()) {
                String path = files.next().getPath().toString();
                byte[] bytes = HDFSUtils.readFile(fs, path);
                sample.unmarshall(bytes);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        index.loadSample(sample);
        System.out.println("BUILD: scan sample time = " + ((System.nanoTime() - startTime) / 1E9));
        startTime = System.nanoTime();
        index.initProbe();
        System.out.println("BUILD: index building time = " + ((System.nanoTime() - startTime) / 1E9));
        System.out.println("Types: "+ Arrays.toString(index.dimensionTypes));

        startTime = System.nanoTime();
        byte[] indexBytes = index.marshall();
        OutputStream out = HDFSUtils.getHDFSOutputStream(fs, hdfsDir+"/index", (short)1, 536870912);
        try {
            out.write(indexBytes);
            out.flush();
            out.close();
            System.out.println("BUILD: index writing time = " + ((System.nanoTime() - startTime) / 1E9));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

