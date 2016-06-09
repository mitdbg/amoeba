package core.adapt;

import java.io.IOException;
import java.sql.Timestamp;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.io.ByteStreams;

import core.utils.ConfUtils;
import core.utils.CuratorUtils;
import core.utils.HDFSUtils;

public class HDFSPartition extends Partition {
	private static final long serialVersionUID = 1L;

	protected FileSystem hdfs;
	protected short replication;

	private FSDataInputStream in;
	private long totalSize = 0;
	private long readSize = 0; 
	private long returnSize = 0;
	private final static int MAX_READ_SIZE = 1024 * 1024 * 50;

	private CuratorFramework client;

	public HDFSPartition(FileSystem hdfs, String pathAndPartitionId,
			short replication, CuratorFramework client) {
		super(pathAndPartitionId);
		this.hdfs = hdfs;
		this.replication = replication;
		this.client = client;
	}

	@Override
	public Partition clone() {
		Partition p = new HDFSPartition(hdfs, path + partitionId, replication, client);
		p.bytes = new byte[8192];
		p.state = State.NEW;
		return p;
	}

	public FileSystem getFS() {
		return hdfs;
	}

	public boolean loadNext() {
		try {
			if (totalSize == 0) {
				Path p = new Path(path + "/" + partitionId);
				totalSize = hdfs.getFileStatus(p).getLen();
				in = hdfs.open(p);
			}

			if (readSize < totalSize) {
				bytes = new byte[(int) Math.min(MAX_READ_SIZE, totalSize
						- readSize)];
				ByteStreams.readFully(in, bytes);
				readSize += bytes.length;
				return true;
			} else {
				in.close();
				readSize = 0;
				totalSize = 0;
				return false;
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to read file: " + path + "/"
					+ partitionId);
		}
	}

	@Override
	public boolean load() {
		if (path == null || path.equals(""))
			return false;
		bytes = HDFSUtils.readFile(hdfs, path + "/" + partitionId);
		return true; // load the physical block for this partition
	}

	@Override
	public byte[] getNextBytes() {
		if (readSize <= returnSize) {
			boolean f = loadNext();
			if (!f)
				return null;
		}
		returnSize += bytes.length;
		return bytes;
	}

	@Override
	public void store(boolean append) {
		InterProcessSemaphoreMutex l = CuratorUtils.acquireLock(client,
				"/partition-lock-" + path.hashCode() + "-" + partitionId);
		System.out.println("LOCK: acquired lock,  " + "path=" + path
				+ " , partition id=" + partitionId + " ,size= " + offset );


		String storePath = path + "/" + partitionId;
		if (!path.startsWith("hdfs"))
			storePath = "/" + storePath;

		Path e = new Path(storePath);

		FSDataOutputStream os = null;

		boolean shouldAppend = false;

		try{
			boolean overwrite = !append;
			os = hdfs.create(e, overwrite, hdfs.getConf().getInt("io.file.buffer.size", 4096),
					replication, hdfs.getDefaultBlockSize(e));

			System.out.println("created partition " + partitionId);
		}
		catch(IOException ex){
			shouldAppend = true;
		}

		try{
			if(shouldAppend){
				os = hdfs.append(e);
			}
			os.write(bytes, 0, offset);
			os.flush();
			os.close();
			recordCount = 0;
		}
		catch(IOException ex){
			System.out.println("exception: "
					+ (new Timestamp(System.currentTimeMillis())));
			//throw new RuntimeException(ex.getMessage());
		}
		finally {
			CuratorUtils.releaseLock(l);
			System.out.println("LOCK: released lock " + partitionId);
		}
	}

	@Override
	public void drop() {
		// HDFSUtils.deleteFile(hdfs, path + "/" + partitionId, false);
	}
}
