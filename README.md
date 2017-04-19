# hbase-app

打包方式
mvn clean package -Dskiptest



服务器部署
cp ./target/hbase-app*.jar $HBASE_HOME/lib
stop-hbase.sh;
start-hbase.sh;

客户端测试
export CLASSPATH=`hbase classpath`
java -cp $CLASSPATH zhl.study.hbaseapp.coprocessor.RowCountEndPointServer
