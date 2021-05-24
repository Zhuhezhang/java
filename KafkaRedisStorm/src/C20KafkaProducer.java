import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 将发送过来的信息保存到Kafka队列
 * 
 * @author zhz
 */

public class C20KafkaProducer {
	public void start(String data) {
		try {
			Properties props = new Properties();// 新建一个配置文件对象
			props.put("bootstrap.servers", "localhost:9092");// 用于建立初始连接到kafka集群的"主机/端口对"配置列表（必须）
			props.put("acks", "all");// Producer在确认一个请求发送完成之前需要收到的反馈信息的数量，0表示不会收到，认为已经完成发送（可选）
			props.put("retries", 0);// 若设置大于0的值，则客户端会将发送失败的记录重新发送（可选）
			props.put("batch.size", 16384);// 控制发送数据批次的大小（可选）
			props.put("linger.ms", 1);// 发送时间间隔，producer会将两个请求发送时间间隔内到达的记录合并到一个单独的批处理请求中（可选）
			props.put("buffer.memory", 33554432);// Producer用来缓冲等待被发送到服务器的记录的总字节数（可选）
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");// 关键字的序列化类（必须）
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");// 值的序列化类（必须）

			Producer<String, String> producer = new KafkaProducer<>(props);// 新建一个以上述定义的配置的生产者
			producer.send(new ProducerRecord<String, String>("my_topic", data));// 将数据传到Kafka队列的url主题
			producer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
