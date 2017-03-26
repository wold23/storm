package com.ihydt.bigdata.storm.first;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

/**
 * 第一级spilt bolt ,可以看到,在发射数据到第二层bolt上时候, OutputCollector.emit()并不能指定一个消息id
 * 也就是说,bolt内,没有起始手动定义的ack_fail机制, storm会在spout传入tuple的时候,维护tuple树,在内部发射出去数据的时候,
 * 第二层tuple也就知道上一层tuplpe,storm会维护整棵树上的结果,通过异或,最终得出结果,并且结果通知到了spout. 也不会通知到bolt,因为bolt内部,
 * 也没有ack和fail方法.
 *
 *
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



        // 第一层 bolt 回复spout发送的tuple处理成功
        // collector.ack(input);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 声明的数据名称和发射的一致 上边发射两个 一个是单词 一个是数量
        declarer.declare(new Fields("word","num"));
    }
}
