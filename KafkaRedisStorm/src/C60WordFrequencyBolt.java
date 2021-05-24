import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * 订阅C50 bolt的输出流，实现单词计数，并发送当前计数给下一个C70 bolt
 * 
 * @author zhz
 */
public class C60WordFrequencyBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private HashMap<String, Long> counts = new HashMap<String, Long>();// 存放统计的词频

	@Override
	public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	/**
	 * 对每次发送过来的单词进行统计并发往C70
	 */
	@Override
	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		Long count = counts.get(word);// 通过单词作为key获取个数
		if (count == null) {
			count = 0L;//如果不存在，初始化为0
		}
		count++;//增加计数
		counts.put(word, count);//存储计数
		collector.emit(new Values(word,count));// 往C70发射
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word","count"));
	}
}
