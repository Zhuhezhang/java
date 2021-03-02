import redis.clients.jedis.Jedis;

/**
 * 将传过来的信息保存到Redis
 * 
 * @author 朱和章
 */

public class C21SaveToRedis {
	public void start(String data) {
		try {
			Jedis jedis = new Jedis("localhost");// 创建Jedis对象：连接本地的Redis服务
			jedis.set("my_key", data);// 存放数据 key value
			jedis.close();// 关闭Redis服务
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}