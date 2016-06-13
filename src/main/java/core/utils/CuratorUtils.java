package core.utils;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class CuratorUtils {

    /**
     * Settings
     */
    private static int baseSleepTimeMills = 10000;
    private static int maxRetries = 4;
    private static int waitTimeSeconds = 10000;

    private static int sessionTimeout = 200000;
    private static int connectionTimeout = 20000;

	/*
     * Curator client utils
	 */

    // public static class MyStateListener implements ConnectionStateListener{
    // public void stateChanged(CuratorFramework arg0, ConnectionState arg1) {
    // }
    // }

    public static CuratorFramework createClient(String zkHosts) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(
                baseSleepTimeMills, maxRetries);
        CuratorFramework client = CuratorFrameworkFactory.newClient(zkHosts,
                sessionTimeout, connectionTimeout, retryPolicy);
        // try {
        // client.getZookeeperClient().blockUntilConnectedOrTimedOut();
        // } catch (InterruptedException e) {
        // e.printStackTrace();
        // }
        return client;
    }

    public static CuratorFramework createAndStartClient(String zkHosts) {
        CuratorFramework client = createClient(zkHosts);
        client.start();
        return client;
    }

    public static void stopClient(CuratorFramework client) {
        client.close();
    }

	/*
	 * Lock utils
	 */

    public static InterProcessSemaphoreMutex acquireLock(
            CuratorFramework client, String lockPath) {

        // make sure that the zookeeper client is connected
        int connTries = 0, maxConnTries = 100;
        while (!(client.getZookeeperClient().isConnected())
                && connTries < maxConnTries) {
            ThreadUtils.sleep(500);
            connTries++;
        }
        if (!(client.getZookeeperClient().isConnected()))
            throw new RuntimeException("The zookeeper cient is not connected!");

        InterProcessSemaphoreMutex lock = new InterProcessSemaphoreMutex(
                client, lockPath);

        try {
            if (lock.acquire(waitTimeSeconds, TimeUnit.SECONDS))
                return lock;
            else {
                System.out.println("Time out: Failed to obtain lock: "
                        + lockPath);
                // return null;
                throw new RuntimeException("Failed to obtain lock: " + lockPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
            // return null;
            throw new RuntimeException("Failed to obtain lock: " + lockPath
                    + "\n " + e.getMessage());
        }
    }

    public static void releaseLock(InterProcessSemaphoreMutex lock) {
        try {
            lock.release();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to unlock " + lock + "\n "
                    + e.getMessage());
        }
    }

    /*
     * Counter utils
     */
    public static SharedCount createCounter(String hosts, String counterPath) {
        CuratorFramework client = createClient(hosts);
        return new SharedCount(client, counterPath, 0);
    }

    public static SharedCount createAndStartCounter(String zkHosts,
                                                    String counterPath) {
        SharedCount c = createCounter(zkHosts, counterPath);
        try {
            c.start();
            return c;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to start the counter: "
                    + counterPath + "\n" + e.getMessage());
        }
    }

    public static SharedCount createAndStartCounter(CuratorFramework client,
                                                    String counterPath) {
        SharedCount c = new SharedCount(client, counterPath, 0);
        try {
            c.start();
            return c;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to start the counter: "
                    + counterPath + "\n" + e.getMessage());
        }
    }

    public static int getCounter(SharedCount c) {
        return c.getCount();
    }

    public static int getCounter(CuratorFramework client, String counterPath) {
        SharedCount c = new SharedCount(client, counterPath, 0);
        try {
            c.start();
            int val = c.getCount();
            c.close();
            return val;

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to add the counter: "
                    + counterPath + "\n" + e.getMessage());
        }
    }

    public static void addCounter(SharedCount c, int increment) {
        int oldVal = c.getCount();
        try {
            c.setCount(oldVal + increment);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to increment the counter: " + c);
        }
    }

    public static void addCounter(CuratorFramework client, String counterPath,
                                  int increment) {
        SharedCount c = new SharedCount(client, counterPath, 0);
        try {
            c.start();
            int oldVal = c.getCount();
            c.setCount(oldVal + increment);
            c.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to add the counter: "
                    + counterPath + "\n" + e.getMessage());
        }
    }

    public static void setCounter(CuratorFramework client, String counterPath,
                                  int value) {
        SharedCount c = new SharedCount(client, counterPath, 0);
        try {
            c.start();
            c.setCount(value);
            c.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to add the counter: "
                    + counterPath + "\n" + e.getMessage());
        }
    }

    public static void deleteAll(CuratorFramework client, String path,
                                 String prefix) {
        try {
            // client.start();
            List<String> children = client.getChildren().forPath(path);
            // System.out.println(children);
            for (String child : children) {
                if (child.startsWith(prefix)) {
                    // System.out.println("deleting: "+path + child);
                    client.delete().deletingChildrenIfNeeded()
                            .forPath(path + child);
                }
            }
            // client.close();

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to delete path: " + path);
        }
    }

    public static void printAll(CuratorFramework client, String path,
                                String prefix) {
        client.start();
        List<String> children;
        try {
            children = client.getChildren().forPath(path);
            for (String child : children) {
                if (child.startsWith(prefix)) {
                    SharedCount c = new SharedCount(client, path + child, 0);
                    c.start();
                    System.out.println(path + child + "\t" + c.getCount());
                    c.close();
                }
            }
            client.close();
        } catch (Exception e) {
            e.printStackTrace();

        }
    }

    public static void main(String[] args) {
        String zookeeperHost = "localhost";
        String deletePath = "/";
        String prefix = "partition-count";

        CuratorUtils.printAll(CuratorUtils.createClient(zookeeperHost),
                deletePath, prefix);
    }
}
