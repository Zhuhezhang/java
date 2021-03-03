import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * 订阅C40/C50发射的tuple流，实现行数统计
 * 
 * @author zhuhezhang
 */

public class C52RowCountBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private String inputField;// 要读取的tuple字段名
	private long rowCount = 0L;// 行数

	public C52RowCountBolt(String inputField) {
		this.inputField = inputField;
	}

	@Override
	public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	/**
	 * 统计行数并发往C70
	 */
	@Override
	public void execute(Tuple input) {
		String dataLine = input.getStringByField(inputField);
		if (dataLine != null)
			rowCount++;
		collector.emit(new Values(rowCount));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("rowCount"));
	}
}
