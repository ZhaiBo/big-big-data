package ink.zhaibo.storm;

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
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * 求和
 */
public class SumApp {
    private static class SumSpout extends BaseRichSpout {

        SpoutOutputCollector collector;
        int number = 0;

        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.collector = spoutOutputCollector;
        }

        public void nextTuple() {
            Utils.sleep(1000);
            this.collector.emit(new Values(++number));
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("number"));
        }
    }

    private static class SumBolt extends BaseRichBolt {

        int sum = 0;

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        }

        public void execute(Tuple tuple) {
            Integer number = tuple.getIntegerByField("number");
            sum += number;
            System.out.println("number is :[ " + number + " ],sum is :[ " + sum + " ]");
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        }
    }

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("SumSpout", new SumSpout());
        builder.setBolt("SumBolt", new SumBolt()).shuffleGrouping("SumSpout");

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("SumApp", new Config(), builder.createTopology());
    }
}
