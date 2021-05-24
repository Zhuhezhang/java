import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

/**
 * 屏幕显示统计结果保存至redis
 * 
 * @author zhz
 */

public class C70ReportBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private HashMap<String, Long> counts = new HashMap<String, Long>();// 保存数据的单词和对应的计数
	private String redisKey = "my_count";// 存放统计结果的redis key值
	private long wordCount;// 数据的字数
	private long rowCount;// 数据的行数
	private String WORD_COUNT_BOLT_ID;// 单词统计boltID
	private String FREQUENCY_COUNT_BOLT_ID;// 词频统计boltID
	private String currentSpout;// 当前spout(kafka/redis)
	private static int index = 0;

	public C70ReportBolt(String WORD_COUNT_BOLT_ID, String FREQUENCY_COUNT_BOLT_ID) {
		this.WORD_COUNT_BOLT_ID = WORD_COUNT_BOLT_ID;
		this.FREQUENCY_COUNT_BOLT_ID = FREQUENCY_COUNT_BOLT_ID;
	}

	/**
	 * 获得C51、C51、C60发射过来的数据
	 */
	@Override
	public void execute(Tuple input) {
		if (FREQUENCY_COUNT_BOLT_ID.equals(input.getSourceComponent())) {// 数据的词频统计
			if (input.getSourceComponent().contains("KAFKA"))
				currentSpout = "KAFKA";
			else
				currentSpout = "REDIS";
			String word = input.getStringByField("word");
			Long count = input.getLongByField("count");
			counts.put(word, count);
			System.out.println(counts);// 实时输出统计结果
		} else if (WORD_COUNT_BOLT_ID.equals(input.getSourceComponent()))// 数据的字数统计
			wordCount = input.getLongByField("wordCount");
		else// 数据的行数统计
			rowCount = input.getLongByField("rowCount");
	}

	/**
	 * Storm在终止一个bolt之前会调用这个方法，本程序在topology关闭时输出最终的计数结果
	 */
	@Override
	public void cleanup() {
		index++;
		Jedis jedis = new Jedis("localhost");

		System.out.println("---------- " + currentSpout + " FINAL COUNTS -----------");// kafka数据源统计的结果
		System.out.println("词频统计：");
		if (index == 1)
			jedis.set(redisKey, "---------- " + currentSpout + " FINAL COUNTS -----------\n");
		else
			jedis.append(redisKey, "---------- " + currentSpout + " FINAL COUNTS -----------\n");
		jedis.append(redisKey, "词频统计：\n");

		ArrayList<HashMap.Entry<String, Long>> list = new ArrayList<HashMap.Entry<String, Long>>(counts.entrySet());// 转换为list
		list.sort(new Comparator<HashMap.Entry<String, Long>>() {// 定义匿名内部类，覆盖compare方法，根据值进行排序
			@Override
			public int compare(HashMap.Entry<String, Long> o1, HashMap.Entry<String, Long> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});
		for (Map.Entry<String, Long> map : list) {// redis逐个保存并输出统计并排序后的结果
			jedis.append(redisKey, map.getKey() + ": " + map.getValue() + "\n");
			System.out.println(map.getKey() + ": " + map.getValue());
		}

		jedis.append(redisKey, "\n字数：" + wordCount);
		jedis.append(redisKey, "\n行数：" + rowCount + "\n\n");
		System.out.println("\n字数：" + wordCount);
		System.out.println("行数：" + rowCount + "\n\n");
		System.out.println("----------------------------");
		jedis.append(redisKey, "\n----------------------------\n");
		jedis.close();
	}

	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}
