package ink.zhaibo.storm;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * strom实时词频统计
 */
public class WordCountApp {

    /**
     * 从本地读取文件，并发射给Bolt
     */
    public static class DataSourceSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;

        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.collector = spoutOutputCollector;
        }

        public void nextTuple() {
            // 获取所有文件
            Collection<File> files = FileUtils.listFiles(new File("D://wc"),
                    new String[]{"txt"}, true);
            for (File file : files) {
                try {
                    // 获取文件中的所有内容
                    List<String> lines = FileUtils.readLines(file);

                    // 获取文件中的每行的内容
                    for (String line : lines) {
                        // 发射出去
                        this.collector.emit(new Values(line));
                    }

                    FileUtils.moveFile(file, new File(file.getAbsolutePath() + System.currentTimeMillis()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("line"));
        }
    }


    /**
     * 分割Spout发送过来的数据
     */
    public static class SplitBolt extends BaseRichBolt {
        private OutputCollector collector;

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        public void execute(Tuple tuple) {
            String line = tuple.getStringByField("line");
            String[] words = line.split(" ");

            for (String word : words) {
                this.collector.emit(new Values(word));
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }
    }

    /**
     * 汇总
     */
    public static class CountBolt extends BaseRichBolt {

        Map<String, Integer> countMap = new HashMap<String, Integer>();
        private OutputCollector collector;

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        public void execute(Tuple tuple) {
            // 1）获取每个单词
            String word = tuple.getStringByField("word");
            Integer count = countMap.get(word);
            if (count == null) {
                count = 0;
            }

            count++;

            // 2）对所有单词进行汇总
            countMap.put(word, count);

            this.collector.emit(new Values(word, String.valueOf(count)));

            // 3）输出
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~");
            Set<Map.Entry<String, Integer>> entrySet = countMap.entrySet();
            for (Map.Entry<String, Integer> entry : entrySet) {
                System.out.println(entry);
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word", "count"));
        }
    }

    public static void main(String[] args) {
        // 通过TopologyBuilder根据Spout和Bolt构建Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("SplitBolt", new SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt", new CountBolt()).shuffleGrouping("SplitBolt");

        // 创建本地集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountStormTopology",
                new Config(), builder.createTopology());
    }
}
