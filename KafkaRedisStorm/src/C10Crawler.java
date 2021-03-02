import java.io.*;
import java.net.*;

/**
 * 爬取并返回的网站信息
 * 
 * @author 朱和章
 */

public class C10Crawler {
	public String start(String url) {
		StringBuilder dataSB = new StringBuilder(url + '\n');// 网址+返回的数据（使用字符串生成器，由于需要频繁附加字符串）
		try {
			URL urlObject = new URL(url);// 建立URL对象
			URLConnection conn = urlObject.openConnection();// 通过URL对象打开连接
			InputStream is = conn.getInputStream();// 通过连接取得网页返回的数据
			String dataLine;// 爬取的每一行的数据
			BufferedReader br = new BufferedReader(new InputStreamReader(is, "GBK"));// 一般按行读取网页数据，因此用BufferedReader和InputStreamReader把字节流转化为字符流的缓冲流
			while ((dataLine = br.readLine()) != null) {// 按行读取
				dataSB.append(dataLine + '\n');
			}
			br.close();// 关闭BufferedReader
			is.close();// 关闭InputStream
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return dataSB.toString();
	}
}