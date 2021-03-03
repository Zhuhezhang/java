import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.utils.Exit;
import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * 从Kafka队列中读取信息并将该信息作为数据源Spout发送到C50、C51、C52Bolt
 * 
 * @author zhuhezhang
 */

public class C40KafkaSpout extends BaseRichSpout {// BaseRichSpout是ISpout接口和IComponent接口的简单实现，接口对用不到的方法提供了默认的实现
	private static final long serialVersionUID = 1L;// 序列化号
	private String[] dataLine;// 从kafka队列获得并分割后的数据
	private int dataNum = 0;// 数据条数
	private String topic = "my_topic";// kafka主题名
	private SpoutOutputCollector collector;// 用于发射数据流
	private int offset = 0;// 数据偏移量
	private String KAFKA_DATA_FIELD;// kafka数据field字段ID

	public C40KafkaSpout(String KAFKA_DATA_FIELD) {
		this.KAFKA_DATA_FIELD = KAFKA_DATA_FIELD;
		dataLine =  kafkaConsumer().split("\n");// 从kafka队列中获得数据并根据换行符分割
		dataNum = dataLine.length;
	}

	/**
	 * kafka消费者，用于从kafka队列中获得数据
	 */
	public String kafkaConsumer() {
		Properties props = new Properties();// 新建一个配置文件对象
		props.put("bootstrap.servers", "localhost:9092");// 用于建立初始连接到kafka集群的"主机/端口对"配置列表（必须）
		props.put("group.id", "my_group");// 消费者组id（必须）
		props.put("auto.offset.reset", "earliest");// 自动将偏移量重置为最早的偏移量（可选）
		props.put("enable.auto.commit", "true");// 消费者的偏移量将在后台定期提交（可选）
		props.put("auto.commit.interval.ms", "1000");// 如果将enable.auto.commit设置为true，则消费者偏移量自动提交给Kafka的频率（以毫秒为单位）（可选）
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");// 关键字的序列化类（必须）
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");// 值的序列化类（必须）

		String data = null;
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);// 新建一个以上述定义的配置的消费者
		consumer.subscribe(Collections.singletonList(topic));// 订阅消息主题
		ConsumerRecords<String, String> records = consumer.poll(100);
		for (ConsumerRecord<String, String> record : records) {
			data = record.value();
		}
		consumer.close();
		if (data == null)// 如果获得的数据为空，则退出程序
			Exit.exit(0);
		return data;
	}

	/**
	 * 在Spout组件初始化时被调用，这里保存SpoutOutputCollector对象，用于发送tuple
	 */
	@Override
	public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	/**
	 * storm调用这个方法，向输出的collector发出tuple，这里只发送dataNum次数据
	 */
	@Override
	public void nextTuple() {
		if (offset < dataNum) {
			collector.emit(new Values(dataLine[offset]));
			offset++;
		}
	}

	/**
	 * 所有Storm的组件（spout和bolt）都必须实现这个接口 用于告诉Storm流组件将会发出那些数据流，每个流的tuple将包含的字段
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(KAFKA_DATA_FIELD));
	}
}
