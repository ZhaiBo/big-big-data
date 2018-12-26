package ink.zhaibo.storm.integration.jdbc;

import com.google.common.collect.Maps;
import ink.zhaibo.storm.WordCountApp;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Map;

/**
 * Storm集成JDBC
 */
public class WordCountJdbcApp {
    public static void main(String[] args) {
        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost/bigdata-test");
        hikariConfigMap.put("dataSource.user", "root");
        hikariConfigMap.put("dataSource.password", "123456");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

        String tableName = "wc_test";
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withInsertQuery("insert into wc_test values (?,?)")
                .withQueryTimeoutSecs(30);

        // 通过TopologyBuilder根据Spout和Bolt构建Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new WordCountApp.DataSourceSpout());
        builder.setBolt("SplitBolt", new WordCountApp.SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt", new WordCountApp.CountBolt()).shuffleGrouping("SplitBolt");
        builder.setBolt("UserPersistanceBolt", userPersistanceBolt).shuffleGrouping("CountBolt");

        // 创建本地集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("WordCountRedisApp",
                new Config(), builder.createTopology());
    }
}
