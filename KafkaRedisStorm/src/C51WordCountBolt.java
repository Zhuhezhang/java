import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * 订阅C40/C41发射的tuple流，实现字数统计
 * 
 * @author zhuhezhang
 */

public class C51WordCountBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private String inputField;// 要读取的tuple字段名
	private long wordCount = 0L;// 字数

	public C51WordCountBolt(String inputField) {
		this.inputField = inputField;
	}

	@Override
	public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	/**
	 * 字数统计
	 */
	public long getWordsCount(String context) {
		long wordCount = 0L;
		Pattern pattern = Pattern.compile("[^(\\u4e00-\\u9fa5，。《》？；’‘：“”【】、）（……￥！·)]");
		Matcher matcher = pattern.matcher(context);
		while (matcher.find()) {// 统计中文字符
			wordCount++;
		}
		pattern = Pattern.compile("[^(a-zA-Z0-9`\\-=\';.,/~!@#$%^&*()_+|}{\":><?\\[\\])]");
		matcher = pattern.matcher(context);
		while (matcher.find()) {// 统计非中文字符
			wordCount++;
		}
		return wordCount;
	}

	@Override
	public void execute(Tuple input) {
		String dataLine = input.getStringByField(inputField);
		wordCount += getWordsCount(dataLine);
		collector.emit(new Values(wordCount));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("wordCount"));
	}
}
