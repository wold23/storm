package com.ihydt.bigdata.storm.first;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Created by lidan on 17-1-16.
 */
public class FirstWordCount  {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        // topology builder 设置topology 需要的spout和bolt 已经串联关系
        TopologyBuilder builder = new TopologyBuilder();
        // 设置spout  spout名称 spout task数量
        builder.setSpout("spout",new FirstWordCountSqout(),2);

        builder.setBolt("split", new FirstWordCountSpiltBolt(), 4).shuffleGrouping("spout");
        // 统计单词 第一个bolt射出数据后,需要根据单词分组,才能保证数据发送到同一个bolt上,结果是正确的
        // 分组的 标志如"word" 是在spout上声明的
        builder.setBolt("count",new FirstWordCountBolt(),4).fieldsGrouping("split",new Fields("word"));
        StormTopology topology = builder.createTopology();

        // 通过config设置 woker数量
        Config config = new Config();
        config.setNumWorkers(3);
        config.setDebug(true); // debug模式

        // 本地模式运行
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("mywordcount",config,topology);

        // 集群模式运行
        //StormSubmitter.submitTopology("mywordcount",config,topology);
    }
}
