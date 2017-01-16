package com.ihydt.bigdata.storm.first;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by lidan on 17-1-16.
 */
public class FirstWordCountSqout extends BaseRichSpout {
    private SpoutOutputCollector collector ;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void nextTuple() {
        String str = "my name is xiaohei i come from china what is your name";
        // 系统封装成了tuple对象
        collector.emit(new Values(str));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // bolt 并没有用到名称 是否可以不用声明 经过测试,不行,看来storm发射数据,都需要标记数据名称
        // 名称可以和下游名称重复 不过还是应该表明数据
        declarer.declare(new Fields("words"));
    }
}
