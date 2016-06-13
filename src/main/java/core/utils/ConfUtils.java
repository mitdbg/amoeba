package core.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class ConfUtils {
    private Properties p;

    public ConfUtils(String propertiesFile) {
        p = new Properties();
        try {
            p.load(new FileInputStream(propertiesFile));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("could not read the properties file!");
        } catch (IOException e) {
            throw new RuntimeException("could not read the properties file!");
        }
    }

    /**
     * Choose whether to enable optimizer If false, just runs the query If true,
     * tries to optimize the storage layout
     *
     * @return
     */
    public boolean getENABLE_OPTIMIZER() {
        return Boolean.parseBoolean(p.getProperty("ENABLE_OPTIMIZER").trim());
    }

    public String getMACHINE_ID() {
        String machineId = p.getProperty("MACHINE_ID").trim()
                .replaceAll("\\W+", "");
        assert !machineId.equals("");
        return machineId;
    }

    /**********
     * SPARK CONFIG
     **************/
    public String getSPARK_MASTER() {
        return p.getProperty("SPARK_MASTER").trim();
    }

    public String getSPARK_HOME() {
        return p.getProperty("SPARK_HOME").trim();
    }

    /**
     * @return spark application jar filepath
     */
    public String getSPARK_APPLICATION_JAR() {
        return p.getProperty("SPARK_APPLICATION_JAR").trim();
    }

    public String getSPARK_EXECUTOR_MEMORY() {
        return p.getProperty("SPARK_EXECUTOR_MEMORY").trim();
    }

    public String getSPARK_DRIVER_MEMORY() {
        return p.getProperty("SPARK_DRIVER_MEMORY").trim();
    }

    public String getSPARK_TASK_CPUS() {
        return p.getProperty("SPARK_TASK_CPUS").trim();
    }

    /**********
     * ZOOKEEPER CONFIG
     **************/
    public String getZOOKEEPER_HOSTS() {
        return p.getProperty("ZOOKEEPER_HOSTS").trim();
    }

    /********** HADOOP CONFIG **************/
    /**
     * Path to hadoop installation
     *
     * @return
     */
    public String getHADOOP_HOME() {
        return p.getProperty("HADOOP_HOME");
    }

    /**
     * Path to hadoop namenode, eg: hdfs://localhost:9000
     *
     * @return
     */
    public String getHADOOP_NAMENODE() {
        return p.getProperty("HADOOP_NAMENODE").trim();
    }

    /**
     * Get working directory in HDFS.
     *
     * @return filepath to hdfs working dir
     */
    public String getHDFS_WORKING_DIR() {
        return p.getProperty("HDFS_WORKING_DIR").trim();
    }

    public void setHDFS_WORKING_DIR(String path) {
        p.setProperty("HDFS_WORKING_DIR", path);
    }

    /**
     * Get HDFS Replication Factor
     *
     * @return
     */
    public short getHDFS_REPLICATION_FACTOR() {
        return Short
                .parseShort(p.getProperty("HDFS_REPLICATION_FACTOR").trim());
    }
}
