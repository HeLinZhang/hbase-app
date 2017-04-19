# hbase-app

生成RowCount.java

将该文件存放在工程的 src/main/protobuf 目录下。
mkdir $PROJECT_HOME/hhbase-app/src/main/protobuf

mv RowCount.proto $PROJECT_HOME/rowCount/src/main/protobuf
用 Protobuf 编译器将该 proto 定义文件编译为 Java 代码，并放到 Maven 工程下。

cd $PROJECT_HOME/hhbase-app/src/main/protobuf
protoc --java_out=$PROJECT_HOME/hhbase-app/src/main/java RowCount.proto


打包方式
mvn clean package -Dskiptest



服务器部署
cp ./target/hbase-app*.jar $HBASE_HOME/lib
stop-hbase.sh;
start-hbase.sh;

客户端测试
export CLASSPATH=`hbase classpath`
java -cp $CLASSPATH zhl.study.hbaseapp.coprocessor.RowCountEndPointServer
