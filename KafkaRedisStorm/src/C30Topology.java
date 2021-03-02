import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * storm拓扑
 * 
 * @author 朱和章
 */

public class C30Topology {
	private static final String KAFKA_DATA_FIELD = "KAFKA_DATA";// kafka数据field字段ID
	private static final String KAFKA_SPOUT_ID = "KAFKA_SPOUT";// kafka数据源SpoutID
	private static final String KAFKA_SPLIT_WORD_BOLT_ID = "KAFKA_SPLIT_WORD_BOLT";// kafka分割单词boltID
	private static final String KAFKA_WORD_COUNT_BOLT_ID = "KAFKA_WORD_COUNT_BOLT";// kafka单词统计boltID
	private static final String KAFKA_ROW_COUNT_BOLT_ID = "KAFKA_ROW_COUNT_BOLT";// kafka行数统计boltID
	private static final String KAFKA_FREQUENCY_COUNT_BOLT_ID = "KAFKA_FREQUENCY_COUNT_BOLT";// kafka词频统计boltID
	private static final String KAFKA_REPORT_BOLT_ID = "KAFKA_REPORT_BOLT";// kafka输出运算结果boltID
	private static final String KAFKA_TOPOLOGY_NAME = "KAFKA_TOPOLOGY";// kafka拓扑名称

	private static final String REDIS_DATA_FIELD = "REDIS_DATA";// redis数据field字段ID
	private static final String REDIS_SPOUT_ID = "REDIS_SPOUT";// redis数据源SpoutID
	private static final String REDIS_SPLIT_WORD_BOLT_ID = "REDIS_SPLIT_WORD_BOLT";// redis分割单词boltID
	private static final String REDIS_WORD_COUNT_BOLT_ID = "REDIS_WORD_COUNT_BOLT";// redis单词统计boltID
	private static final String REDIS_ROW_COUNT_BOLT_ID = "REDIS_ROW_COUNT_BOLT";// redis行数统计boltID
	private static final String REDIS_FREQUENCY_COUNT_BOLT_ID = "REDIS_FREQUENCY_COUNT_BOLT";// redis词频统计boltID
	private static final String REDIS_REPORT_BOLT_ID = "REDIS_REPORT_BOLT";// redis输出运算结果boltID
	private static final String REDIS_TOPOLOGY_NAME = "REDIS_TOPOLOGY";// redis拓扑名称

	private long sleepTime;// 经过sleepTime毫秒后关闭程序，根据数据长度来设置

	public C30Topology(long sleepTime) {
		this.sleepTime = sleepTime;
	}

	public void start() {
		TopologyBuilder kafkaBuilder = new TopologyBuilder();// 创建了一个kafka的TopologyBuilder实例
		TopologyBuilder redisBuilder = new TopologyBuilder();// 创建了一个redis的TopologyBuilder实例

		kafkaBuilder.setSpout(KAFKA_SPOUT_ID, new C40KafkaSpout(KAFKA_DATA_FIELD), 1);// 设置数据源并设置1个Executeor(线程)，默认1个
		kafkaBuilder.setBolt(KAFKA_SPLIT_WORD_BOLT_ID, new C50SplitBolt(KAFKA_DATA_FIELD), 2).setNumTasks(2)
				.shuffleGrouping(KAFKA_SPOUT_ID);// 设置2个Executeor(线程)和2个Task，shuffleGrouping表示均匀分配任务，C40KafkaSpout -->
													// C50SplitBolt
		kafkaBuilder.setBolt(KAFKA_WORD_COUNT_BOLT_ID, new C51WordCountBolt(KAFKA_DATA_FIELD), 1).setNumTasks(1)
				.shuffleGrouping(KAFKA_SPOUT_ID);// C40KafkaSpout --> C51WordCountBolt
		kafkaBuilder.setBolt(KAFKA_ROW_COUNT_BOLT_ID, new C52RowCountBolt(KAFKA_DATA_FIELD), 1).setNumTasks(1)
				.shuffleGrouping(KAFKA_SPOUT_ID);// C40KafkaSpout --> C52RowCountBolt
		kafkaBuilder.setBolt(KAFKA_FREQUENCY_COUNT_BOLT_ID, new C60WordFrequencyBolt(), 1).setNumTasks(2)
				.fieldsGrouping(KAFKA_SPLIT_WORD_BOLT_ID, new Fields("word"));// fieldsGrouping表示字段相同的tuple会被路由到同一实例中，C50SplitBolt
																				// --> C60WordFrequencyBolt
		kafkaBuilder// globalGrouping表示把Bolt发送的的所有tuple路由到唯一的Bolt，C51WordCountBolt/C52RowCountBolt/C60WordFrequencyBolt
					// --> C70ReportBolt
				.setBolt(KAFKA_REPORT_BOLT_ID,
						new C70ReportBolt(KAFKA_WORD_COUNT_BOLT_ID, KAFKA_FREQUENCY_COUNT_BOLT_ID))
				.globalGrouping(KAFKA_WORD_COUNT_BOLT_ID).globalGrouping(KAFKA_ROW_COUNT_BOLT_ID)
				.globalGrouping(KAFKA_FREQUENCY_COUNT_BOLT_ID);

		redisBuilder.setSpout(REDIS_SPOUT_ID, new C41RedisSpout(REDIS_DATA_FIELD));// 设置数据源
		redisBuilder.setBolt(REDIS_SPLIT_WORD_BOLT_ID, new C50SplitBolt(REDIS_DATA_FIELD), 2).setNumTasks(2)
				.shuffleGrouping(REDIS_SPOUT_ID);// C41RedisSpout --> C50SplitBolt
		redisBuilder.setBolt(REDIS_WORD_COUNT_BOLT_ID, new C51WordCountBolt(REDIS_DATA_FIELD), 1).setNumTasks(1)
				.shuffleGrouping(REDIS_SPOUT_ID);// C41RedisSpout --> C51WordCountBolt
		redisBuilder.setBolt(REDIS_ROW_COUNT_BOLT_ID, new C52RowCountBolt(REDIS_DATA_FIELD), 1).setNumTasks(1)
				.shuffleGrouping(REDIS_SPOUT_ID);// C41RedisSpout --> C52RowCountBolt
		redisBuilder.setBolt(REDIS_FREQUENCY_COUNT_BOLT_ID, new C60WordFrequencyBolt(), 1).setNumTasks(2)
				.fieldsGrouping(REDIS_SPLIT_WORD_BOLT_ID, new Fields("word"));// C50SplitBolt --> C60WordFrequencyBolt

		redisBuilder// C51WordCountBolt/C52RowCountBolt/C60WordFrequencyBolt --> C70ReportBolt
				.setBolt(REDIS_REPORT_BOLT_ID,
						new C70ReportBolt(REDIS_WORD_COUNT_BOLT_ID, REDIS_FREQUENCY_COUNT_BOLT_ID))
				.globalGrouping(REDIS_WORD_COUNT_BOLT_ID).globalGrouping(REDIS_ROW_COUNT_BOLT_ID)
				.globalGrouping(REDIS_FREQUENCY_COUNT_BOLT_ID);

		try {
			LocalCluster cluster = new LocalCluster();// 新建LocalCluster实例
			Config kafkaConfig = new Config();// 用于配置kafka拓扑
			Config redisConfig = new Config();// 用于配置redis拓扑
			cluster.submitTopology(KAFKA_TOPOLOGY_NAME, kafkaConfig, kafkaBuilder.createTopology());// 本地提交运行kafka拓扑
			cluster.submitTopology(REDIS_TOPOLOGY_NAME, redisConfig, redisBuilder.createTopology());// 本地提交运行redis拓扑
			Utils.sleep(sleepTime);// 休眠等待运行结束
			cluster.killTopology(KAFKA_TOPOLOGY_NAME);// 杀掉拓扑
			cluster.killTopology(REDIS_TOPOLOGY_NAME);
			cluster.shutdown();// 关闭cluster
			cluster.close();// 释放资源
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}