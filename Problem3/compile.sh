rm ./*.class
rm ./*.jar
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk.x86_64
export PATH=${JAVA_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:${JAVA_HOME}/lib/opencsv-4.5.jar
/usr/local/hadoop/bin/hadoop com.sun.tools.javac.Main RankLang.java
jar cf RankLang.jar RankLang*.class


