#基于SparkStreaming的实时数据清洗
---
数据清洗， 是整个数据分析过程中不可缺少的一个环节，其结果质量直接关系到模型效果和最终结论。在实际操作中，数据清洗通常会占据分析过程的50%—80%的时间。国外有些学术机构会专门研究如何做数据清洗，相关的书籍也不少。本文的背景是netflow数据流的实时清洗，相比于离线的数据清洗，实时数据清洗更倾向于数据的缺值过滤和数据合法性检查以及对性能的苛求。离线数据清洗，则可以通过牺牲性能的方式借助复杂的处理，对数据进行更加细粒度的清洗。对于实时数据清洗的流程我把它分为三步。

##一.数据清洗的流程
####**第一：数据预处理**
**1.1 数据查看：**数据查看可以大致分为两方面。一是查看数据的元数据信息，包括数据来源，字段解释，字段的逻辑关系。二是从数据源抽取一部分数据，人工查看，加深对数据的了解，发现问题，为后续的数据清洗做准备。<br/>
**1.2 选取处理工具：**鉴于数据实时传输，处理工具选择kafka与SparkStreaming。
####**第二：删除或填充缺失数据**
**2.1 确定缺失值范围：**根据数据的重要程度以及数据的缺失程度评估缺失分数，然后按照缺失比例和字段重要性，分别制定策略。

**2.2 去除不需要的数据：**这一步主要是为了减少数据的冗余。建议做好数据备份，避免后面的业务逻辑需要用到某些字段。或者先在少量数据上试验成功之后再对全量数据进行处理。
2.3 填充缺失内容：某些没有的数据值需要进行填充，避免后续程序在对数据进行分析时出现空值错误。数据填充方法大概分为三种：

	a)以业务知识或经验推测填充缺失值
	b)以同一指标的计算结果（均值、中位数、众数等）填充缺失值
	c)以不同指标的计算结果填充缺失值
前两种方法比较好理解。关于第三种方法，举个最简单的例子：年龄字段缺失，但是有屏蔽后六位的身份证号，从中提取需要的信息。
####**第三：数据内容检测**

**3.1 数据合法性检测：**检测各字段是否与指定内容相符，主要是数值，日期等格式。
	
**3.2 数据业务逻辑检测：**这一部分则是判断数据是否符合逻辑，如：人的年龄一般都在0-120之间。如果有出现200岁的年龄则判断该条数据时异常数据。

##二.实时数据清洗实战

###1.设计思路
我司的实时大数据处理的架构是flume+kafka+sparkstreaming+mysql/hive/hdfs/hbase。数据通过flume收集起来，通过kafka来做缓存和容灾，最后由SparkStreaming来做实时处理。为了使数据清洗从代码中独立出来减小代码间的耦合性，特单独起一个streaming来进行数据清洗，从kafka的topic中拿到数据清洗完成后放入另一个topic中供后续的业务来做处理。这样做的好处是通过牺牲少许的实时性来换取数据的安全性。

###2.主体流程

>连接数据源-->获得数据流-->数据过滤-->数据合法性检测-->数据输出

在代码的实现过程中，连接数据源，获得数据流以及数据过滤都比较简单，最重要的是合法性检测和数据输出，会有一些问题和需要注意的地方，下文都会提到。

###3.具体实现


**3.1 :domain**

为了使程序有更好的可扩展性，把需要处理的数据抽象成一个Metadata对象，封装每个字段的属性。对于任务需要的配置参数和环境变量，抽象成JobInfo对象，对于对象属性的获取，则可以自己通过方法来定义或者直接采取赋值的形式

**3.2 :util**

util部分主要是两个工具类--source和sink(名字随便取)。souce用来从数据源读取数据，sink用来将数据吐到目的地。

**3.3 :shuffle**

shuffle部分则负责数据清洗的主要流程。我的demo主要是数据的过滤和合法性检测，所以涉及的方法主要是类型和格式的检测。这里需要视实际情况来写，也可以尽可能的完善来增加程序的灵活性。鉴于实时数据清洗对于性能的苛求，在清洗方法设计的时候也要尽可能的避免消耗性能比较厉害的方法和操作，比如复杂的正则表达，字符的匹配，类型的转换，以及trycatch代码块的频繁使用。通过更加高效的方法，如位运算，ASCII码比对，等方式提高代码的效率。


###4.部分代码介绍

**4.1KafkaSink**

KafkaSink是用来创建kafka的producer从而把数据导入到kafka中

	class KafkaSink[K,V](createProducer: () => KafkaProducer[K, V]) extends Serializable{
		lazy val producer = createProducer()
	  	def send(topic: String, key: K, value: V): Future[RecordMetadata] =
	    producer.send(new ProducerRecord[K, V](topic, key, value))
	  	def send(topic: String, value: V): Future[RecordMetadata] =
	    producer.send(new ProducerRecord[K, V](topic, value))
	}

在SparkStreaming中创建kafka的producer的一个典型问题是因为producer对象无法序列化在集群执行时task无法序列化产生的问题。对此我们可以通过懒加载的方式把producer的实例化放在各个节点来执行，有几个节点就有几个producer一方面可以提高并行度，另一方面也可以避免因无法序列化造成的问题。

**4.2 数值类型检测**

数值类型检测是数据类型检测的一个基础，Long，Int，Double都是基于这个方法来展开

对于数值类型的检测思路大概有三种，一是正则来匹配也是最容易想到的，不过正则需要解析和匹配所以性能肯定不理想；二是通过toInt等方法来实现，通过trycatch方法根据是否抛出异常来判断是不是数值类型，首先受trycatch性能的影响这种方法的效率比简单的正则的性能还要差，而且在catch代码块中嵌入业务逻辑不是一个好的编程习惯，所以这种方法显然也不合适；第三种就是如上面代码片段实例的通过ASCII码的比对来实现。这种方法无疑比前两种方法要快的多。经过测试比之简单的正则匹配和trycatch性能要高3到5倍以上，数据量来到千万级以上是表现更加明显。


  	def isNum(value: String):Boolean = {
    	for (c <- 0 to (value.size-1)){
      	  if(value.charAt(c)==13){
        	 return true
      	  }
      	  if (value.charAt(c)<48|value.charAt(c)>57) {
        	 return false
      	  }
    	}
    	return true
  	}
这个有个地方需要注意，因为linux文本编辑器与window编辑器的不同，它会默认在行尾添加\r来进行换行，这在isNum方法执行时因为ASCII码的比对会对程序造成误导，通过简单的处理来规避这种情况，也可以直接把\r用空串替换掉(作者没有替换成功，可以再试试)。

**4.3 日期类型的匹配**

日期类型的转换不管在什么地方都是一个相当消耗性能的地方，对于日期类型的检测更加要注意。如果在网上查看网友给的推荐多是通过正则来匹配，或者是simpledateformat的parse方法来将数据转换成日期类型，通过trycatch来捕获异常。上文我们讨论过正则和trycatch的性能不太理想，如果是次数少的话倒是无所谓。不过在数据的实时处理的过程中如果处理速度过慢可能会造成上游的数据挤压，到最后会发生数据丢失或者雪崩现象。因此对于日期类型的匹配还需要自己独立设计，这块的性能优化视代码质量可以提高五倍以上。

通过翻看simpledataformat的源码，我把日期类型的检测分成两个部分分别是compile和isDate。compile负责编译用户给定的日期格式，因为一种日志的日期格式都是不会常常变动的，把这部分从实时流处理中抽取出来放在初始化部分来完成从而来保证实时流处理的性能。compile部分代码如下所示：

	def compile(c:String):Array[Char]={
    	val TYPE_YEAR_y = 121
    	val TYPE_YEAR_Y = 89
    	val TYPE_MONTH_M= 77
    	val TYPE_DAY_dd = 100
    	val TYPE_HOUR_H = 72
    	val TYPE_MINUTS_m = 109
    	val TYPE_SECONDS_s = 115
    	var info = new Array[Char](c.length)
    	var TAG_Parrtan = true

		for(x <- 0 to c.trim.length-1){
      		c.charAt(x) match{
        		case TYPE_YEAR_y    =>info(x)='y' //17
        		case TYPE_YEAR_Y    =>info(x)='y' //17
        		case TYPE_MONTH_M   =>info(x)='M' //33
        		case TYPE_DAY_dd    =>info(x)='d'//49
        		case TYPE_HOUR_H    =>info(x)='H'//65
        		case TYPE_MINUTS_m  =>info(x)='m'//81
        		case TYPE_SECONDS_s =>info(x)='s'//97
        		case '/'            =>info(x)='/'
        		case '-'            =>info(x)='-'
        		case ' '            =>info(x)=' '
       	 		case ':'            =>info(x)=':'
        		case _              =>TAG_Parrtan=false
      		}
    	}
    	if(TAG_Parrtan){
      		info
    	}else{
     		 null
    	}
	}

compile的功能很简单，主要是对用户传来的日期格式进行编译看是否是合法的日期格式。把最后结果封装到char数组，最后再根据数据流读取到的日期数据转换成的日期数组进行匹配查看数据是否符合定义好的规则。isDate代码如下所示：

	def isDate(patten:Array[Char],dateSource:String):Boolean={
    	if(patten!=null&&dateSource!=null&&patten.length==dateSource.length){
      		var TAG_SOURSE = true
      		val source = dateSource.toCharArray
      		for( num<- 0 to (patten.length-1) if source(num)<48|source(num)>57){
        	if(patten(num)!=source(num)){
         		TAG_SOURSE = false
          		return TAG_SOURSE
        	}
      }
      for( num<- 0 to (patten.length-1) if num!=patten.length-1&&(patten(num+1))!='/'&&(patten(num+1))!='-'&&(patten(num+1))!=' '&&(patten(num+1))!=':'
        &&patten(num)!='/'&&patten(num)!='-'&&patten(num)!=' '&&patten(num)!=':'){
       		patten(num) match{
          		case 'M'=>{
            		if((source(num)-'0')*10+(source(num+1)-'0')>12){
              		TAG_SOURSE = false
              		return TAG_SOURSE
            		}
          		}
          		case 'd'=>{
            		if((source(num)-'0')*10+(source(num+1)-'0')>31){
              		TAG_SOURSE = false
             	 	return TAG_SOURSE
            		}
          		}
          		case 'H'=>{
            		if((source(num)-'0')*10+(source(num+1)-'0')>24){
              		TAG_SOURSE = false
              		return TAG_SOURSE
            		}
          		}
          		case 'm'=>{
            		if((source(num)-'0')*10+(source(num+1)-'0')>60){
             	 	TAG_SOURSE = false
              		return TAG_SOURSE
            		}
          		}
          		case 's'=>{
            		if((source(num)-'0')*10+(source(num+1)-'0')>60){
              		TAG_SOURSE = false
              		return TAG_SOURSE
            		}
          		}

          		case _=>
        		}
        
      	}

      	TAG_SOURSE

    	}else{
     		 false
    	}
	}
大概思路是通过循环过滤来匹配定义好的规则，这种方法只能做到简单的日期的匹配。更细粒度的匹配需要的逻辑太过复杂服药扩展。性能方面比正则和trycatch快五倍左右。


##三.总结
最后将程序打成jar包，导入其他程序可以正常使用。该程序只关注数据清洗的部分，对于缺失值的删除或填充则是通过重写shuffle方法来实现。对于填充的值则需要用户自己根据业务逻辑也好，指标或者其统计方式来获得，我们只需要给他提供传值的方法就行，无需关注如何获得到的值。

程序还有很多可以优化的部分，这只是一个大概的思路和测试。希望读者有好的想法或思路可以告知，互相学习。