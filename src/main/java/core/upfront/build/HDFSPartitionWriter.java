package core.upfront.build;

import core.utils.ConfUtils;
import core.utils.HDFSUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;

public class HDFSPartitionWriter extends PartitionWriter {

    private ConfUtils cfg;

    private short replication;
    private FileSystem hdfs;

    private CuratorFramework client;

    public HDFSPartitionWriter(String partitionDir, int bufferPartitionSize,
                               short replication, ConfUtils cfg, CuratorFramework client) {
        super(partitionDir, bufferPartitionSize);
        this.replication = replication;
        this.client = client;
        createHDFS(cfg);
    }

    public HDFSPartitionWriter(String partitionDir, ConfUtils cfg) {
        super(partitionDir);
        createHDFS(cfg);
    }

    @Override
    public PartitionWriter clone() throws CloneNotSupportedException {
        HDFSPartitionWriter w = (HDFSPartitionWriter) super.clone();
        w.createHDFS(this.cfg);
        return w;
    }

    private void createHDFS(ConfUtils cfg) {
        this.cfg = cfg;
        this.hdfs = HDFSUtils.getFSByHadoopHome(this.cfg.getHADOOP_HOME());
    }

    @Override
    public void createPartitionDir() {
        try {
            hdfs.mkdirs(new Path(partitionDir));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deletePartitionDir() {
        try {
            hdfs.delete(new Path(partitionDir), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected OutputStream getOutputStream(String path) {
        return HDFSUtils.getBufferedHDFSOutputStream(hdfs, path, replication,
                bufferPartitionSize, client);
    }
}
