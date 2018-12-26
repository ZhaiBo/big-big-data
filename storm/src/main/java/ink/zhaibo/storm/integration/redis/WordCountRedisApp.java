package ink.zhaibo.storm.integration.redis;

import ink.zhaibo.storm.WordCountApp;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.topology.TopologyBuilder;

/**
 * strom统计结果写入redis
 * 两种方式:
 * 1、实现LookUpMapper和StoreMapper
 * 2、继承RedisStoreBolt
 */
public class WordCountRedisApp {

    private static final String LOCAL_HOST = "127.0.0.1";
    private static final int PORT = 6379;

    public static void main(String[] args) {
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost(LOCAL_HOST).setPort(PORT).build();
        WordCountRedisStoreMapper storeMapper = new WordCountRedisStoreMapper();

        // 通过TopologyBuilder根据Spout和Bolt构建Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new WordCountApp.DataSourceSpout());
        builder.setBolt("SplitBolt", new WordCountApp.SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt", new WordCountApp.CountBolt()).shuffleGrouping("SplitBolt");
        builder.setBolt("storeBolt", new RedisStoreBolt(poolConfig, storeMapper)).shuffleGrouping("CountBolt");

        // 创建本地集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("WordCountRedisApp",
                new Config(), builder.createTopology());
    }
}
