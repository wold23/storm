package com.ihydt.bigdata.storm.first;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * Created by lidan on 17-1-16.
 */
public class FirstWordCountSpiltBolt extends BaseRichBolt {
    private OutputCollector collector ;
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        // 对数据进行切分
        // 由于数据我就发了一组 可以只获取第一组
        String value = (String)input.getValue(0);
        for (String word : StringUtils.split(value, " ")) {
            collector.emit(new Values(word,1));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 声明的数据名称和发射的一致 上边发射两个 一个是单词 一个是数量
        declarer.declare(new Fields("word","num"));
    }
}
