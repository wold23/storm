package com.ihydt.bigdata.storm.first;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 增加ack_fail机制保证数据处理正确
 *
 * 在spout发出tuple时候,增加messageId,storm 的acker线程会追踪tuple处理过程,在下游bolt处理过程中,
 * 通过业务逻辑控制ack或fail或系统超时决定,tuple是否处理成功,storm会回调spout定义的ack和fail方法,至于数据
 * 失败了,如何处理,由我们自己决定
 *
 *
 * Created by lidan on 17-1-16.
 */
public class FirstWordCountSqout extends BaseRichSpout {
    private SpoutOutputCollector collector ;
    private Map<String,Values> cache = new HashMap<String, Values>();
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void nextTuple() {
        String str = "my name is xiaohei i come from china what is your name";
        // 系统封装成了tuple对象
        // collector.emit(new Values(str));

        // 只有设置了messageId storm才会使用acker task帮我们追踪tuple
        String messageId = UUID.randomUUID().toString();
        Values values = new Values(str);
        collector.emit(values,messageId);
        // 缓存数据到map内 失败的时候 再次重新发送tuple
        cache.put(messageId,values);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // bolt 并没有用到名称 是否可以不用声明 经过测试,不行,看来storm发射数据,都需要标记数据名称
        // 名称可以和下游名称重复 不过还是应该表明数据
        declarer.declare(new Fields("words"));
    }


    @Override
    public void ack(Object msgId) {
       // tuple 下游数据处理成功,回调该方法
        cache.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {

        // tuple 下游数据处理失败,回调该方法
        Values values = cache.get(msgId);
        // 重新发射
        System.out.println(msgId+"消息重发!"+values.get(0));
        collector.emit(values,msgId);
    }
}
