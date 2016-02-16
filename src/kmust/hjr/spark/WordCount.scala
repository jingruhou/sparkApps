package kmust.hjr.spark

import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by houjingru on 2016/2/16.
  */
object WordCount {
  def main(args: Array[String]) {
    /**
     * 第一步：
     * 创建Spark配置对象SparkConf,设置Spark程序的运行时的配置信息
     * 比如：通过setMaster设置程序要链接的Spark集群的Master的URL
     * 如果设置为local，则代表Spark程序的本地运行模式
     */
    val conf = new SparkConf();//创建SparkConf对象
    conf.setAppName("WordCountApp")//设置应用程序的名称，在程序运行的监控界面可以看到该名称
    conf.setMaster("local")//设置运行模式为local模式，不需要安装配置Spark集群

    /**
      * 第二步：
      * 创建SparkContext对象
      * SparkContext是Spark程序所有功能的唯一入口，无论采用Scala、Java、Python、R等都必须有一个SparkContext对象
      *
      * SparkContext核心作用：初始化 Spark应用程序 运行所需要的 核心组件，包括DAGScheduler、TaskScheduler、SchedulerBackend,
      * 同时还会负责Spark程序往Master注册程序等
      * SparkContext是整个Spark应用程序中最为至关重要的对象
      */
    val sc = new SparkContext(conf)//创建SparkContext对象，通过传入SparkConf实例对象来定制Spark运行的具体参数和配置信息

    /**
      * 第三步：
      * 根据具体的数据来源（HDFS,HBas, Local FS,DB,S3等）
      * 通过SparkContext创建RDD
      *
      * RDD的创建基本有三种方式：根据外部的数据来源（例如：HDFS）、根据Scala集合、由其它的RDD操作
      *数据会被RDD划分为一系列的Partitions，分配到每个Partition的数据属于一个Task的处理范畴
      */
    //val lines：RDD[String] = sc.textFile("E://SparkData//building-spark.md",1)/
    val lines = sc.textFile("E://SparkData//building-spark.md",1)//读取本地文件，并设置为一个Partition

    /**
      * 第四步：
      * 对初始的RDD进行Transformation级别的处理，
      * 例如 map、filter等高阶函数等的编程，来进行具体的数据计算
      *
      * 4.1 将每一行的字符串拆分成单个的单词
      */
    val words = lines.flatMap{line => line.split(" ")}//对每一行的字符串进行单词拆分，并把所有行的拆分结果通过flat合并成为一个大的单词集合

    /**
      * 第四步：
      * 对初始的RDD进行Transformation级别的处理，
      * 例如 map、filter等高阶函数等的编程，来进行具体的数据计算
      *
      * 4.2  在单词拆分的基础上对每个单词实例计数为1
      *       也就是 word => (word, 1)
      */
    val pairs = words.map{ word => (word, 1)}

    /**
      * 第四步：
      * 对初始的RDD进行Transformation级别的处理，
      * 例如 map、filter等高阶函数等的编程，来进行具体的数据计算
      *
      * 4.3  在每个单词实例计数为1的基础之上，
      *      统计每个单词在文件中出现的总次数
      */
      val WordCounts = pairs.reduceByKey(_+_)//对相同的key，进行value的累计（包括Local和Reducer级别的Reduce）

      WordCounts.foreach(wordNumberPair => println(wordNumberPair._1 + " : "+ wordNumberPair._2))

      sc.stop()
  }
}
