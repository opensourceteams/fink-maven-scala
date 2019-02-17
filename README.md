# flink 开发笔记


### 创建flink java 项目
```aidl
mvn archetype:generate                               \
      -DarchetypeGroupId=org.apache.flink              \
      -DarchetypeArtifactId=flink-quickstart-java     \
      -DarchetypeVersion=1.7.1
      -DgroupId=com.opensourceteams \
      -DartifactId=flink-maven-java \
      -Dversion=0.0.1 \
      -Dpackage=com.opensourceteams.module.bigdata.flink  \
      -DinteractiveMode=false

```

### 创建flink scala项目
```aidl
mvn archetype:generate                               \
      -DarchetypeGroupId=org.apache.flink              \
      -DarchetypeArtifactId=flink-quickstart-scala     \
      -DarchetypeVersion=1.7.1
      -DgroupId=com.opensourceteams \
      -DartifactId=flink-maven-scala-2 \
      -Dversion=0.0.1 \
      -Dpackage=com.opensourceteams.module.bigdata.flink  \
      -DinteractiveMode=false

```


### maven 运行某个类
```aidl
mvn exec:java -Dexec.mainClass=wikiedits.WikipediaAnalysis

```

###  查看jar中文件列表
```aidl
jar tvf test.jar 

```


### 运行 jar 到 Flink 集群
```aidl
 flink run -c  com.opensourceteams.module.bigdata.flink.example.stream.worldcount.nc.SocketWindowWordCount    ./flink-maven-scala-2-0.0.1.jar  

```

### scala 版Flink WordCount单词统计
- https://github.com/opensourceteams/fink-maven-scala/blob/master/md/wordCount-scala.md




