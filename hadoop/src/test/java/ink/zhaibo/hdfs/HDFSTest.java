package ink.zhaibo.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

/**
 * hdfs java api
 */
public class HDFSTest {

    public static final String HDFS_PATH = "hdfs://bigdata:8020";

    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Before
    public void setUp() throws Exception {
        System.out.println("HDFSApp.setUp");
        configuration = new Configuration();
        //解决远程主机datanode找不到的问题,返回主机名名称，而不是内网地址
        configuration.set("dfs.client.use.datanode.hostname", "true");
        configuration.set("fs.defaultFS", HDFS_PATH);
        configuration.set("user","hadoop");
        fileSystem = FileSystem.get(configuration);
    }

    @After
    public void tearDown() throws Exception {
        configuration = null;
        fileSystem = null;

        System.out.println("HDFSApp.tearDown");
    }

    /**
     * 创建HDFS目录
     */
    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /**
     * 创建文件
     */
    @Test
    public void create() throws Exception {
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/c.txt"));
        output.write("no hello hadoop111111111111111".getBytes());
        output.flush();
        output.close();
    }

    /**
     * 查看HDFS文件的内容
     */
    @Test
    public void cat() throws Exception {
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/test/c.txt"));
        IOUtils.copyBytes(in, System.out, 1024);
        in.close();
    }


    /**
     * 重命名
     */
    @Test
    public void rename() throws Exception {
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/aaa.txt");
        fileSystem.rename(oldPath, newPath);
    }

    /**
     * 上传文件到HDFS
     *
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile() throws Exception {
        Path localPath = new Path("D://test-hdfs.txt");
        Path hdfsPath = new Path("/hdfsapi/test");
        fileSystem.copyFromLocalFile(localPath, hdfsPath);
    }

    /**
     * 带进度条上传文件到HDFS
     */
    @Test
    public void copyFromLocalFileWithProgress() throws Exception {
        InputStream in = new BufferedInputStream(
                new FileInputStream(
                        new File("/Users/rocky/source/spark-1.6.1/spark-1.6.1-bin-2.6.0-cdh5.5.0.tgz")));

        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/spark-1.6.1.tgz"),
                new Progressable() {
                    public void progress() {
                        System.out.print(".");  //带进度提醒信息
                    }
                });


        IOUtils.copyBytes(in, output, 4096);
    }


    /**
     * 下载HDFS文件
     */
    @Test
    public void copyToLocalFile() throws Exception {
        Path localPath = new Path("D://test-down.txt");
        Path hdfsPath = new Path("/hdfsapi/test/a.txt");
        fileSystem.copyToLocalFile(hdfsPath, localPath);
    }

    /**
     * 查看某个目录下的所有文件
     */
    @Test
    public void listFiles() throws Exception {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));

        for(FileStatus fileStatus : fileStatuses) {
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();

            System.out.println(isDir + "\t" + replication + "\t" + len + "\t" + path);
        }

    }

    /**
     * 删除
     */
    @Test
    public void delete() throws Exception{
        fileSystem.delete(new Path("/"), true);
    }
}
