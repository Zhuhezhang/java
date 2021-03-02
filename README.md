Linux环境下Apache Storm的使用：爬取指定网站信息并将信息存进redis和kafka，再将两者读出的数据作为spout，分别对两者的数据词频统计、行数统计、字数统计，并将所得结果存入redis内存数据库，并观察这两个情况的时延、吞吐量。注意：两个数据源两个拓扑。
@[TOC](目录)	

# 1．环境搭建
## 1.1安装JDK
1. 从[官网](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html)下载合适的安装包，这里使用的是安装包是jdk-8u271-linux-x64.tar.gz，注意一定要使用此版本，否则无法正常使用storm
2. 解压tar -zxvf jdk-8u271-linux-x64.tar.gz
3. 设置环境变量：打开文件vim /etc/profile并在最前面添加(其中第一行的路径为jdk文件解压后的路径)
```powershell
export JAVA_HOME=/usr/lib/jvm/jdk
export JRE_HOME=${JAVA_HOME}/jre
export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib  
export PATH=${JAVA_HOME}/bin:$PATH
```
4. 使用source /etc/profile使修改后的文件生效（最好重启电脑）
## 1.2安装eclipse
由于使用jdk8，导致原来使用的9月份版本的eclipse无法使用，因此从[官网](https://www.eclipse.org/downloads/download.php?file=/technology/epp/downloads/release/2020-03/R/eclipse-java-2020-03-R-linux-gtk-x86_64.tar.gz)上下载之前发布的版本，解压后直接打开里面的执行文件就可以了，这里使用的安装包是eclipse-java-2020-03-R-linux-gtk-x86_64.tar.gz。需要注意的是若是重新装过jdk，可能eclipse项目使用的版本可能是之前的，需要在eclipse中修改过来。

## 1.3 安装、打开zookeeper服务
 1. [官网](https://zookeeper.apache.org/releases.html)下载安装包，这里使用的是apache-zookeeper-3.6.1-bin.tar.gz并解压
 2. 进入到解压后的目录，cd apache-zookeeper-3.6.1-bin
 3. mkdir data，新建一个文件夹
 4. cp conf/zoo_sample.cfg conf/zoo.cfg，复制文件，名称一定要是zoo.cfg
 5. vim conf/zoo.cfg，打开该文件，并将里面的dataDir的路径修改为新建的data目录
 6. bin/zkServer.sh Start，启动服务，若显示STARTED则表示启动成功
## 1.4安装、打开Kafka服务
1. 从[官网](https://www.apache.org/dyn/closer.cgi?path=/kafka/2.6.0/kafka_2.13-2.6.0.tgz)下载安装包，这里使用的安装包是kafka_2.13-2.6.0.tgz
2. 解压下载的安装包tar -zxf kafka_2.13-2.6.0.tgz
3. 切换到解压后的文件的目录，cd kafka_2.13-2.6.0
4. 最后再通过命令bin/kafka-server-start.sh config/server.properties启动Kafka服务（在启动kafka之前需要保证zookeeper服务已启动）
## 1.5安装、打开Redis服务
1. [官网](https://redis.io/download)下载安装包，这里使用的是redis-6.0.9.tar.gz并解压
2. 切换到解压后的目录
3. make
4. 完成后使用./src/redis-server ./redis.conf启动Redis服务即可开始使用
## 1.6 安装、打开Storm服务
1. [官网](https://storm.apache.org/downloads.html)下载安装包，本次下载的是apache-storm-2.2.0.tar.gz并解压
2. cd apache-storm-2.2.0，进入解压后的文件
3. mkdi data，新建一个目录
4. vim ./conf/storm.yaml，打开并修改配置文件，如下图，其中图中的路径为2.6.3新建的data文件的路径，注意各个字段中的空格![在这里插入图片描述](https://img-blog.csdnimg.cn/20210302205828318.png#pic_center)

5. bin/storm nimbus，启动nimbus（在此之前必须确保zookeeper已经启动）
6. bin/storm supervisor，启动supervisor
7. bin/storm ui，可以启动ui（可选），浏览器输入localhost:8888即可查看相关信息
8. jps，查看是否启动成功，若成功则会显示Nimbus, Supervisor, QuorumPeerMain（zookeeper的后台进程）
## 1.7 添加maven依赖
在maven项目的pom.xml文件中添加以下导入kafka、storm、redis、ansj相关jar包

```xml
<dependency>
			<groupId>org.apache.storm</groupId>
			<artifactId>storm-core</artifactId>
			<version>2.2.0</version>
		</dependency>
		<dependency>
			<groupId>org.apache.kafka</groupId>
			<artifactId>kafka-clients</artifactId>
			<version>1.0.0</version>
		</dependency>
		<dependency>
			<groupId>redis.clients</groupId>
			<artifactId>jedis</artifactId>
			<version>3.4.0</version>
		</dependency>
		<dependency>
			<groupId>org.ansj</groupId>
			<artifactId>ansj_seg</artifactId>
			<version>5.1.6</version>
		</dependency>
```
# 2．程序使用说明
在程序运行之前需要通过命令行启动zookeeper/kafka/redis/storm服务，也就是实验环境搭建部分的服务启动，然后直接在eclipse中运行即可。而爬虫获得的数据在kafka中保存在名为my_topic的主题、在redis内存数据库中保存的键值为my_key，而运行程序的结果则保存在redis中键值名为my_count。需要注意的是当爬取不同的网站时，需要根据网站信息的多少来设定休眠时长，设置过长会浪费时间；太短则会导致服务一直重启，也会浪费时间，而且时间过短可能会使程序出错。当没有实时输出统计信息时就代表运行结束，但是不可直接强制关闭程序，否则会无法输出最后的统计信息，等到指定时间程序会自动关闭并在控制台输出、保存最后的统计结果。
# 3．运行截图
![图 1 kafkaSpout控制台部分输出1](https://img-blog.csdnimg.cn/20210302210302695.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3FxXzQzNzk0NjMz,size_16,color_FFFFFF,t_70#pic_center)
图 1 kafkaSpout控制台部分输出1
![在这里插入图片描述](https://img-blog.csdnimg.cn/20210302210514199.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3FxXzQzNzk0NjMz,size_16,color_FFFFFF,t_70#pic_center)
图2 kafkaSpout控制台部分输出2
![在这里插入图片描述](https://img-blog.csdnimg.cn/20210302210622805.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3FxXzQzNzk0NjMz,size_16,color_FFFFFF,t_70#pic_center)
图 3 redisSpout控制台部分输出1
![在这里插入图片描述](https://img-blog.csdnimg.cn/20210302210635303.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3FxXzQzNzk0NjMz,size_16,color_FFFFFF,t_70#pic_center)
图4 redisSpout控制台部分输出2
![在这里插入图片描述](https://img-blog.csdnimg.cn/20210302210648301.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3FxXzQzNzk0NjMz,size_16,color_FFFFFF,t_70#pic_center)
图 5 redis数据库部分输出1
![在这里插入图片描述](https://img-blog.csdnimg.cn/2021030221065946.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3FxXzQzNzk0NjMz,size_16,color_FFFFFF,t_70#pic_center)
图6 redis数据库部分输出2
![在这里插入图片描述](https://img-blog.csdnimg.cn/20210302210712683.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3FxXzQzNzk0NjMz,size_16,color_FFFFFF,t_70#pic_center)
图7 redis数据库部分输出3
# 4．总体设计
C00Main.java类为程序入口，首先先将爬取指定网站的信息分别存储进kafka、redis，分别对应C10Crawler.java、C20KafkaProducer.java、C21SavaToRedis.java。C30Topology用于定义storm拓扑结构，C40KafkaSpout.java、C41RedisSpout.java分别利用kafka、redis将爬取的信息读取出来并作为两个独立的spout分别发往词频统计（C50SplitBolt.java用于将发射过来的数据分割成一个一个单词并将分割后的单词发射给C60WordFrequencyBolt.java用于最后的统计）、行数统计（C52RowCountBolt.java）、字数统计（C51WordCountBolt.java）的bolt，也就是2个数据流对应6个spout、2个拓扑，其中spout的数据的逐行发射。再将分别将每个拓扑的3个spout汇聚到一个C70ReportBolt.java，用于输出、保存统计的信息。其中统计信息的输出、保存是在数据统计完成、休眠时间到了在释放拓扑资源时才会输出。
![在这里插入图片描述](https://img-blog.csdnimg.cn/20210302210752729.jpg?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3FxXzQzNzk0NjMz,size_16,color_FFFFFF,t_70#pic_center)

# 5．详细设计	
## 5.1 C00Main.java
该类为程序入口。首先定义两个常量，分别是爬取的网站的网址以及SLEEP_TIME，也就是经过这么多毫秒后关闭程序，需要根据爬取的网站信息多少来设置时长，否则过长会浪费时间；过短则会影响程序正常运行。利用C10Crawler.java爬取并返回网站的信息，并将此信息利用C20KafkaProducer.java、C21SaveToRedis.java分别往kafka队列发送数据、redis保存数据，再开始通过C30Topology定义storm拓扑并开始统计、输出信息，最后再利用exit退出程序。
## 5.2 C10Crawler.java
该类爬取指定的网站信息并返回爬取的信息。通过start入口方法传入参数url（爬取网站网址），由于爬取的网站信息读取是逐行读取的，对字符串的连接较多，所以这里使用字符串生成器StringBuilder更为节省时间，然后将爬取的网站网址添加在首行。通过网址建立URL对象，利用其方法openConnection打开连接并返回URLConnection对象，再通过该对象的getInputStream方法连接取得网页返回的数据，这里返回InputStream对象。将该对象转化为BufferedReader对象，并指定GBK编码，利用while进行按行读取并利用字符串生成器append方法逐行添加。数据读取完成后依次关闭BufferedStream, InputStream。以上代码放在try-catch语句块中用于处理异常信息，然后以字符串的形式返回读取到的信息。
## 5.3 C20KafkaProducer.java
该类将通过入口方法start的形参传进来的信息然后将其保存到Kafka队列的名为my_topic的主题中。在入口方法start处，新建一个Properties配置文件对象设置bootstrap.s
ervers为localhost:9092用于建立初始连接到kafka集群的"主机/端口对"配置列表（必须）；acks 为all，表示Producer在确认一个请求发送完成之前需要收到的反馈信息的数量，0表示不会收到，认为已经完成发送；retries为0，若设置大于0的值，则客户端会将发送失败的记录重新发送（可选）；batch.size为16384，控制发送数据批次的大小（可选）；linger.ms为1，发送时间间隔，producer会将两个请求发送时间间隔内到达的记录合并到一个单独的批处理请求中（可选）；buffer.memory为33554432，Producer用来缓冲等待被发送到服务器的记录的总字节数（可选）；key.serializer为org.apache.kafka.common.serialization.StringSeria
lizer，关键字的序列化类（必须）；value.serializer为org.apache.kafka.common.serialization.St
ringSerializer，值的序列化类（必须）。
	然后利用以上的配置新建一个Kafka生产者，并利用send方法将数据往Kafka队列指定的主题的发送，最后close关闭即可。
## 5.4 C21SaveToRedis.java
该类将传过来的信息保存到Redis数据库，键值为my_key。该类的入口方法包含要保存的数据。首先创建Jedis对象，连接本地的Redis服务，利用该对象的set方法存放数据，然后利用close关闭Redis服务即可。
## 5.5 C30Topology.java
该类定义storm的拓扑，组织spout和bolt之间的逻辑关系。分别定义kafka、redis数据源的数据field字段、spoutID、分割单词boltID、单词统计boltID、行数统计boltID、词频统计boltID、输出运算结果boltID、拓扑名称这些常量，通过构造函数传入关闭拓扑的时间，根据数据的长度设置。由于以kafka、redis为数据源的拓扑结构一致，因此这里只说明kafka拓扑的定义。
	在start方法中创建一个kafka的TopologyBuilder实例，再利用其方法setSpout设置数据源并设置1个Executeor(线程)，默认1个，如果设置两个线程，那么将会读取数据两次，以此类推。分别调用三次setBolt方法设置数据源的流向，分别流向C50SplitBolt.java、C51WordCountBolt、C52RowCountBolt，这里同时还指定执行的线程个数以及tast个数，而shuffleGrouping表示均匀分配任务。而C50SplitBolt.java再将处理过的数据发往C60WordFrequencyBolt.java，而fieldsGrouping表示字段相同的tuple会被路由到同一实例中。再次利用setBolt方法将C51WordCountBolt/C52RowCountBolt/C60WordFrequencyBolt三个bolt的数据发射到C70ReportBolt.java，globalGrouping表示把Bolt发送的的所有tuple路由到唯一的Bolt。同样的方法再定义redis数据源的流向。
	利用新建的LocalCluster实例、kafka/redis拓扑的配置对象Config，利用前者的submitTopology方法本地提交定义的kafka/redis两个拓扑，利用sleep方法休眠等待运行结束，之后利用killTopology杀掉拓扑同时并利用close方法释放占用的资源。

## 5.6 C40KafkaSpout.java
该类从Kafka队列中读取信息并将该信息作为数据源Spout。继承自BaseRichSpout，它是ISpout接口和IComponent接口的简单实现，接口对用不到的方法提供了默认的实现。首先定义一些变量，包括从kafka队列获得并分割后的数据、分割后的数据条数、读取kafka队列的主题名、用于往下一个bolt发射数据流的SpoutOutputCollector对象、数据偏移量、kafka数据field字段ID。利用构造函数传入kafka数据的field字段ID作为输出field字段ID，同时利用方法kafkaConsumer从队列中拉取数据并返回，对返回的数据根据换行符进行分割，并将分割后的保存的数组长度作为数据条数。
	对于kafkaConsumer方法，首先新建一个配置文件对象，bootstrap.servers字段设置为localhost:9092，用于建立初始连接到kafka集群的"主机/端口对"配置列表（必须）；group.id为my_group，消费者组id（必须）；auto.offset.reset为earliest，自动将偏移量重置为最早的偏移量（可选）；enable.auto.commit为true，消费者的偏移量将在后台定期提交（可选）；auto.commit.interval.ms为1000，如果将enable.auto.commit设置为true，则消费者偏移量自动提交给Kafka的频率（以毫秒为单位）（可选）；key.deserializer为org.apache.kafka.commo
n.serialization.StringDeserializer，关键字的序列化类（必须）；value.deserializer 为org.apach
e.kafka.common.serialization.StringDeserializer，值的序列化类（必须）。
	利用上述配置新建一个Kafka消费者，利用方法subscribe订阅指定名称的主题。再利用poll方法获取队列中的数据集，再利用for结构输出即可，同时close消费者。若数据为空，则直接退出程序，否则返回获得的数据。
	覆盖BaseRichSpout的open方法，该方法在Spout组件初始化时被调用，这里保存SpoutOutputCollector对象，用于发送tuple。覆盖BaseRichSpout的nextTuple方法，storm调用这个方法，向输出的collector发出tuple。这里只发送dataNum次数据，也就是利用emit方法将数据一行一行发射出去。覆盖BaseRichSpout的declareOutputFields方法，所有Storm的组件（spout和bolt）都必须实现这个接口 用于告诉Storm流组件将会发出那些数据流，每个流的tuple将包含的字段，这里为KAFKA_DATA_FIELD。

## 5.7 C41RedisSpout.java
该方法从Redis中读出数据并发射给订阅的Bolt。大部分代码、思路同C40KafkaSpout.java基本一样，唯一不同的就是此数据源利用的是getDataFromRedis方法从redis中获取数据，首先创建Jedis对象：连接本地的Redis服务，通过该对象的get方法获取数据若数据为空则退出程序，否则返回读取的数据。

## 5.8 C50SplitBolt.java
该类将获得的数据分割单词，并一个单词一单词发射给下一个bolt，继承自BaseRichBolt，类似于前面BaseRichSpout，只不过前者是bolt的，后者是spout的。通过构造函数获得要订阅的fieldID，prepare方法同前面所说的open类似，用于bolt的初始化。接下来是本人定义的方法getWords，通过形参将传进来的字符串分割一个一个单词并并以ArrayList的形式返回。这里主要是利用的ansj库的ToAnalysis.parse方法区分出单词/词语出来。再利用split方法根据逗号分割字符串，同时过滤掉空格、空白字符以及标点符号，将符合条件的单词添加到ArrayList当中。
	覆盖接口当中的execute方法，该方法在运行期间不断执行，这里利用方法input.getStr
ingByField接收发射过来的数据，然后调用getWords方法分割单词并逐个往下一个bolt发射数据。覆盖declareOutputFields方法，声明fieldID为word。

## 5.9 C51WordCountBolt.java
该类订阅spout发射的tuple流，实现字数统计。和上面说过的相似的就不再说了，显得冗余。主要说一下不同的地方。execute方法通过getStringByField、filedID获取发射过来的数据，每次有数据过来，则通过getWordCount方法统计字数并和此前统计的字数相加获得总字数并发往下一个bolt。对于方法getWordCount，利用Pattern.compile初始化对象Pattern，再利用该对象的matcher方法返回Matcher对象，然后当matcher.find()==true，也就找到匹配的，每次找到则加一。利用同样的方法统计非中文字符并相加，得到的总字数返回。声明此bolt的fieldID为wordCount。

## 5.10 C52RowCountBolt.java
该类订阅spout发射的tuple流，实现行数统计。在execute方法获得发射过来的数据，由于数据是一行一行发送过来的，所以如果不为空，则统计行数的变量加一，再通过emit方法将统计的数据发往下一个bolt。声明此bolt的fieldID为rowCount。

## 5.11 C60WordFrequencyBolt.java
该类订阅C50SplitBolt的输出流，实现单词计数，并发送当前计数给C70ReportBolt。execute方法中获得发射过来的单词后通过单词作为HashMap的key获得统计的单词个数，如果为空，代表之前没有统计过此单词，则将它额个数加1并利用put方法存进HashMap中，再往C70发射单词以及对应的统计个数。声明此bolt的fieldID为word和count，也就是发射一对数据。
	
## 5.12 C70ReportBolt.java
该方法通过C51/C52/C60发射过来的数据处理之后屏幕输出显示统计的最终结果并保存至redis，键值为my_count。通过构造函数传入单词统计boltID、词频统计boltID。Execute方法中，通过Tuple对象的getSourceComponent可以获得此数据的boltID进而判断是词频统计还是行数统计还是字数统计的信息。如果当中包含KAFKA，则源头数据spout为kafka，并将当前spout，也就是currentSpout设置为KAFKA，否则为REDIS，用于区分数据源spout。
覆盖方法cleanup，Storm在终止一个bolt之前会调用这个方法，本程序在topology关闭时输出、保存最终的计数结果，也就是会显示、保存在execute中变量的最后的值，其中还通过定义匿名内部类并覆盖重写方法compare，使得词频的统计可以根据value值由大到小进行排序输出、保存。由于这是数据流的终点，所以就没必要声明发射的fieldID，但是方法declareOutputFields还是要写出来，prepare也是一个道理。
# 6．存在问题
每次重新启动各项相关服务后运行的第1次运行都会出现从Kafka队列中读取的数据是空的，之后的运行就完全没问题了。虽然知道是因为kafka consumer的问题，但是该bug目前仍然没有办法解决。
# 7. 源代码
## 7.1 C00Main.java
```java
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
 * @author 朱和章
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

```
## 7.2 C10Crawler.java
```java
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

```
## 7.3 C20KafkaProducer.java
```java
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 将发送过来的信息保存到Kafka队列
 * 
 * @author 朱和章
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
```
## 7.4 C21SaveToRedis.java
```java
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
```
## 7.5 C30Topology.java
```java
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * storm拓扑
 * 
 * @author 朱和章
 */

public class C30Topology {
	private static final String KAFKA_DATA_FIELD = "KAFKA_DATA";// kafka数据field字段ID
	private static final String KAFKA_SPOUT_ID = "KAFKA_SPOUT";// kafka数据源SpoutID
	private static final String KAFKA_SPLIT_WORD_BOLT_ID = "KAFKA_SPLIT_WORD_BOLT";// kafka分割单词boltID
	private static final String KAFKA_WORD_COUNT_BOLT_ID = "KAFKA_WORD_COUNT_BOLT";// kafka单词统计boltID
	private static final String KAFKA_ROW_COUNT_BOLT_ID = "KAFKA_ROW_COUNT_BOLT";// kafka行数统计boltID
	private static final String KAFKA_FREQUENCY_COUNT_BOLT_ID = "KAFKA_FREQUENCY_COUNT_BOLT";// kafka词频统计boltID
	private static final String KAFKA_REPORT_BOLT_ID = "KAFKA_REPORT_BOLT";// kafka输出运算结果boltID
	private static final String KAFKA_TOPOLOGY_NAME = "KAFKA_TOPOLOGY";// kafka拓扑名称

	private static final String REDIS_DATA_FIELD = "REDIS_DATA";// redis数据field字段ID
	private static final String REDIS_SPOUT_ID = "REDIS_SPOUT";// redis数据源SpoutID
	private static final String REDIS_SPLIT_WORD_BOLT_ID = "REDIS_SPLIT_WORD_BOLT";// redis分割单词boltID
	private static final String REDIS_WORD_COUNT_BOLT_ID = "REDIS_WORD_COUNT_BOLT";// redis单词统计boltID
	private static final String REDIS_ROW_COUNT_BOLT_ID = "REDIS_ROW_COUNT_BOLT";// redis行数统计boltID
	private static final String REDIS_FREQUENCY_COUNT_BOLT_ID = "REDIS_FREQUENCY_COUNT_BOLT";// redis词频统计boltID
	private static final String REDIS_REPORT_BOLT_ID = "REDIS_REPORT_BOLT";// redis输出运算结果boltID
	private static final String REDIS_TOPOLOGY_NAME = "REDIS_TOPOLOGY";// redis拓扑名称

	private long sleepTime;// 经过sleepTime毫秒后关闭程序，根据数据长度来设置

	public C30Topology(long sleepTime) {
		this.sleepTime = sleepTime;
	}

	public void start() {
		TopologyBuilder kafkaBuilder = new TopologyBuilder();// 创建了一个kafka的TopologyBuilder实例
		TopologyBuilder redisBuilder = new TopologyBuilder();// 创建了一个redis的TopologyBuilder实例

		kafkaBuilder.setSpout(KAFKA_SPOUT_ID, new C40KafkaSpout(KAFKA_DATA_FIELD), 1);// 设置数据源并设置1个Executeor(线程)，默认1个
		kafkaBuilder.setBolt(KAFKA_SPLIT_WORD_BOLT_ID, new C50SplitBolt(KAFKA_DATA_FIELD), 2).setNumTasks(2)
				.shuffleGrouping(KAFKA_SPOUT_ID);// 设置2个Executeor(线程)和2个Task，shuffleGrouping表示均匀分配任务，C40KafkaSpout -->
													// C50SplitBolt
		kafkaBuilder.setBolt(KAFKA_WORD_COUNT_BOLT_ID, new C51WordCountBolt(KAFKA_DATA_FIELD), 1).setNumTasks(1)
				.shuffleGrouping(KAFKA_SPOUT_ID);// C40KafkaSpout --> C51WordCountBolt
		kafkaBuilder.setBolt(KAFKA_ROW_COUNT_BOLT_ID, new C52RowCountBolt(KAFKA_DATA_FIELD), 1).setNumTasks(1)
				.shuffleGrouping(KAFKA_SPOUT_ID);// C40KafkaSpout --> C52RowCountBolt
		kafkaBuilder.setBolt(KAFKA_FREQUENCY_COUNT_BOLT_ID, new C60WordFrequencyBolt(), 1).setNumTasks(2)
				.fieldsGrouping(KAFKA_SPLIT_WORD_BOLT_ID, new Fields("word"));// fieldsGrouping表示字段相同的tuple会被路由到同一实例中，C50SplitBolt
																				// --> C60WordFrequencyBolt
		kafkaBuilder// globalGrouping表示把Bolt发送的的所有tuple路由到唯一的Bolt，C51WordCountBolt/C52RowCountBolt/C60WordFrequencyBolt
					// --> C70ReportBolt
				.setBolt(KAFKA_REPORT_BOLT_ID,
						new C70ReportBolt(KAFKA_WORD_COUNT_BOLT_ID, KAFKA_FREQUENCY_COUNT_BOLT_ID))
				.globalGrouping(KAFKA_WORD_COUNT_BOLT_ID).globalGrouping(KAFKA_ROW_COUNT_BOLT_ID)
				.globalGrouping(KAFKA_FREQUENCY_COUNT_BOLT_ID);

		redisBuilder.setSpout(REDIS_SPOUT_ID, new C41RedisSpout(REDIS_DATA_FIELD));// 设置数据源
		redisBuilder.setBolt(REDIS_SPLIT_WORD_BOLT_ID, new C50SplitBolt(REDIS_DATA_FIELD), 2).setNumTasks(2)
				.shuffleGrouping(REDIS_SPOUT_ID);// C41RedisSpout --> C50SplitBolt
		redisBuilder.setBolt(REDIS_WORD_COUNT_BOLT_ID, new C51WordCountBolt(REDIS_DATA_FIELD), 1).setNumTasks(1)
				.shuffleGrouping(REDIS_SPOUT_ID);// C41RedisSpout --> C51WordCountBolt
		redisBuilder.setBolt(REDIS_ROW_COUNT_BOLT_ID, new C52RowCountBolt(REDIS_DATA_FIELD), 1).setNumTasks(1)
				.shuffleGrouping(REDIS_SPOUT_ID);// C41RedisSpout --> C52RowCountBolt
		redisBuilder.setBolt(REDIS_FREQUENCY_COUNT_BOLT_ID, new C60WordFrequencyBolt(), 1).setNumTasks(2)
				.fieldsGrouping(REDIS_SPLIT_WORD_BOLT_ID, new Fields("word"));// C50SplitBolt --> C60WordFrequencyBolt

		redisBuilder// C51WordCountBolt/C52RowCountBolt/C60WordFrequencyBolt --> C70ReportBolt
				.setBolt(REDIS_REPORT_BOLT_ID,
						new C70ReportBolt(REDIS_WORD_COUNT_BOLT_ID, REDIS_FREQUENCY_COUNT_BOLT_ID))
				.globalGrouping(REDIS_WORD_COUNT_BOLT_ID).globalGrouping(REDIS_ROW_COUNT_BOLT_ID)
				.globalGrouping(REDIS_FREQUENCY_COUNT_BOLT_ID);

		try {
			LocalCluster cluster = new LocalCluster();// 新建LocalCluster实例
			Config kafkaConfig = new Config();// 用于配置kafka拓扑
			Config redisConfig = new Config();// 用于配置redis拓扑
			cluster.submitTopology(KAFKA_TOPOLOGY_NAME, kafkaConfig, kafkaBuilder.createTopology());// 本地提交运行kafka拓扑
			cluster.submitTopology(REDIS_TOPOLOGY_NAME, redisConfig, redisBuilder.createTopology());// 本地提交运行redis拓扑
			Utils.sleep(sleepTime);// 休眠等待运行结束
			cluster.killTopology(KAFKA_TOPOLOGY_NAME);// 杀掉拓扑
			cluster.killTopology(REDIS_TOPOLOGY_NAME);
			cluster.shutdown();// 关闭cluster
			cluster.close();// 释放资源
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
```
## 7.6 C40KafkaSpout.java
```java
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.utils.Exit;
import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * 从Kafka队列中读取信息并将该信息作为数据源Spout发送到C50、C51、C52Bolt
 * 
 * @author 朱和章
 */

public class C40KafkaSpout extends BaseRichSpout {// BaseRichSpout是ISpout接口和IComponent接口的简单实现，接口对用不到的方法提供了默认的实现
	private static final long serialVersionUID = 1L;// 序列化号
	private String[] dataLine;// 从kafka队列获得并分割后的数据
	private int dataNum = 0;// 数据条数
	private String topic = "my_topic";// kafka主题名
	private SpoutOutputCollector collector;// 用于发射数据流
	private int offset = 0;// 数据偏移量
	private String KAFKA_DATA_FIELD;// kafka数据field字段ID

	public C40KafkaSpout(String KAFKA_DATA_FIELD) {
		this.KAFKA_DATA_FIELD = KAFKA_DATA_FIELD;
		dataLine =  kafkaConsumer().split("\n");// 从kafka队列中获得数据并根据换行符分割
		dataNum = dataLine.length;
	}

	/**
	 * kafka消费者，用于从kafka队列中获得数据
	 */
	public String kafkaConsumer() {
		Properties props = new Properties();// 新建一个配置文件对象
		props.put("bootstrap.servers", "localhost:9092");// 用于建立初始连接到kafka集群的"主机/端口对"配置列表（必须）
		props.put("group.id", "my_group");// 消费者组id（必须）
		props.put("auto.offset.reset", "earliest");// 自动将偏移量重置为最早的偏移量（可选）
		props.put("enable.auto.commit", "true");// 消费者的偏移量将在后台定期提交（可选）
		props.put("auto.commit.interval.ms", "1000");// 如果将enable.auto.commit设置为true，则消费者偏移量自动提交给Kafka的频率（以毫秒为单位）（可选）
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");// 关键字的序列化类（必须）
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");// 值的序列化类（必须）

		String data = null;
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);// 新建一个以上述定义的配置的消费者
		consumer.subscribe(Collections.singletonList(topic));// 订阅消息主题
		ConsumerRecords<String, String> records = consumer.poll(100);
		for (ConsumerRecord<String, String> record : records) {
			data = record.value();
		}
		consumer.close();
		if (data == null)// 如果获得的数据为空，则退出程序
			Exit.exit(0);
		return data;
	}

	/**
	 * 在Spout组件初始化时被调用，这里保存SpoutOutputCollector对象，用于发送tuple
	 */
	@Override
	public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	/**
	 * storm调用这个方法，向输出的collector发出tuple，这里只发送dataNum次数据
	 */
	@Override
	public void nextTuple() {
		if (offset < dataNum) {
			collector.emit(new Values(dataLine[offset]));
			offset++;
		}
	}

	/**
	 * 所有Storm的组件（spout和bolt）都必须实现这个接口 用于告诉Storm流组件将会发出那些数据流，每个流的tuple将包含的字段
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(KAFKA_DATA_FIELD));
	}
}
```
## 7.7 C41RedisSpout.java
```java
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
 * @author 朱和章
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
```
## 7.8 C50SplitBolt.java
```java
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
 * @author 朱和章
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
```
## 7.9 C51WordCountBolt.java
```java
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
 * @author 朱和章
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
```
## 7.10 C52RowCountBolt.java
```java
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
 * @author 朱和章
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
```
## 7.11 C60WordFrequencyBolt.java
```java
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
 * @author 朱和章
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
```
## 7.12 C70ReportBolt.java
```java
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
 * @author 朱和章
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
```
# 8. 项目地址
[https://github.com/Zhuhezhang/java](https://github.com/Zhuhezhang/java)
