import redis.clients.jedis.Jedis;
import java.util.Map;

import org.apache.kafka.common.utils.Exit;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * 从Redis中读出数据并发往C50、C51、C52Bolt
 * 
 * @author zhuhezhang
 */

public class C41RedisSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;
	private String key = "my_key";// redis的key值
	private String ip = "localhost";// redis的IP地址
	private String[] dataLine;// 数据
	private int dataNum = 0;// 数据条数
	private int offset = 0;// 数据偏移量
	private SpoutOutputCollector collector;// 用于发射数据流
	private String REDIS_DATA_FIELD;// redis输出字段field

	public C41RedisSpout(String REDIS_DATA_FIELD) {
		this.REDIS_DATA_FIELD = REDIS_DATA_FIELD;
		dataLine = getDataFromRedis().split("\n");// 从redis获得数据
		dataNum = dataLine.length;
	}

	/**
	 * 从redis获得数据
	 */
	public String getDataFromRedis() {
		String data = null;
		try {
			Jedis jedis = new Jedis(ip);// 创建Jedis对象：连接本地的Redis服务
			data = jedis.get(key);// 存放数据 key value
			jedis.close();// 关闭Redis服务
			if (data == null)// 如果获得的数据为空，则退出程序
				Exit.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return data;
	}

	@Override
	public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		if (offset < dataNum) {
			collector.emit(new Values(dataLine[offset]));
			offset++;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(REDIS_DATA_FIELD));
	}
}
