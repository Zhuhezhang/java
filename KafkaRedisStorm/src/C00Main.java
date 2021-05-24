import org.apache.kafka.common.utils.Exit;

/**
 * 题目：
 * 该程序爬取指定网站的信息并存进redis和kafka，再从两者读出数据并利用
 * storm分别对两者的数据进行词频统计、行数统计、字数统计，并将所的结果存入redis
 * 
 * 程序结构：
 * C10爬取并返回数据，通过C20、C21分别保存到kafka和redis，C30为storm的拓扑，C40、C41
 * 分别从kafka和redis中获得数据并作为spout数据源都分别发往C50、C51、C52bolt，对应功能
 * 单词分割、字数统计、行数统计，C50将分割的单词发往C60进行词频统计，然后将以kafka和redis
 * 为数据源所统计的数据，也就是两者的C60、C51、C52都发往C70输出统计结果，同时保存到redis,
 * 一个spout对应的是一个拓扑
 * 
 * @author zhz
 */

public class C00Main {
	private final static String URL = "https://new.qq.com/omn/20201229/20201229A06GUL00.html";//爬取的网站网址
	private final static long SLEEP_TIME = 15000L;// 经过SLEEP_TIME毫秒后关闭程序，根据数据长度来设置

	public static void main(String[] args) {
		String dataToProcess = new C10Crawler().start(URL);//爬取并返回网站信息
    	new C20KafkaProducer().start(dataToProcess);//往kafka队列发送数据
    	new C21SaveToRedis().start(dataToProcess);//往redis保存数据
    	new C30Topology(SLEEP_TIME).start();//运行storm拓扑
		Exit.exit(0);// 退出程序
	}
}
