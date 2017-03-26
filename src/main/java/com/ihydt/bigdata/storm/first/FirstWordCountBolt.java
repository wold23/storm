package com.ihydt.bigdata.storm.first;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lidan on 17-1-16.
 */
public class FirstWordCountBolt extends BaseRichBolt{
    // map 由同一个worker共享,一个task是一个线程,一个woker由多个task并发执行
    private Map<String,Integer> result = new HashMap<String, Integer>();

    private OutputCollector collector ;
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {

        // 随机失败 测试ack fail机制
        if (System.currentTimeMillis() % 3 == 0){
            System.out.println(input.getMessageId()+":消息失败");
            collector.fail(input);
            return ;
        }



        String word = input.getStringByField("word");// 通过声明取值 由框架帮助强制转换
        Integer count = input.getInteger(1);// 通过顺序取值

        Integer sum = result.get(word);
        sum = sum == null ? 1 :sum+count;



        System.err.println("线程"+Thread.currentThread().getName()+" 输出 : "+word+" 数量: "+sum);
        result.put(word,sum);
        // 应答 spout
        collector.ack(input);

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 没有输出结果 就不用声明了
    }
}
