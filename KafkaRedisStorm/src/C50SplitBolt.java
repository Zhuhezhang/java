import java.util.ArrayList;
import java.util.Map;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * 订阅spout发射的tuple流，实现分割单词，并发往C60
 * 
 * @author zhz
 */

public class C50SplitBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private String inputField;// 要读取的tuple字段名

	public C50SplitBolt(String inputField) {
		this.inputField = inputField;
	}

	@Override
	public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
	}

	 /**
     * 根据字符串分割出所有单词并以ArrayList返回
     */
    public ArrayList<String> getWords(String context) {
    	ArrayList<String> list = new ArrayList<String>();
         String result = ToAnalysis.parse(context).toStringWithOutNature();
         String[] words = result.split(",");
         for(String word: words){
             String str = word.trim();//过滤空格
             if (str.equals(""))// 过滤空白字符
                 continue;
             else if(str.matches("\\p{Punct}"))// 过滤标点符号
                 continue;
             list.add(word);
         }
         return list;
    }
 
	/**
	 * 单词一个一个发往C60
	 */
	@Override
	public void execute(Tuple input) {
		String data = input.getStringByField(inputField);
		for (String word : getWords(data)) {
			collector.emit(new Values(word));//向下一个bolt发射数据
		}		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
}
