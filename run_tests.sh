#!/bin/bash

if (( $# != 1 ))
then
    echo "Usage: run_tests <lib_jars_dir>"
    exit
fi

CURRENT_DIR="$(pwd)"
SRC_DIR=./src
JARS=$1/kbase/auth/kbase-auth-1398468950-3552bb2.jar:$1/servlet/servlet-api-2.5.jar:$1/jackson/jackson-annotations-2.2.3.jar:$1/jackson/jackson-core-2.2.3.jar:$1/jackson/jackson-databind-2.2.3.jar:$1/jetty/jetty-all-7.0.0.jar:$1/syslog4j/syslog4j-0.9.46.jar:$1/ini4j/ini4j-0.5.2.jar:$1/apache_commons/commons-io-2.4.jar:$1/apache_commons/commons-fileupload-1.2.2.jar:$1/derby/derby-10.10.1.1.jar:$1/junit/junit-4.9.jar:$1/easymock/easymock-3.2.jar:$1/kbase/common/kbase-common-0.0.3.jar
CLASSPATH="-classpath $JARS"
CLASSES_DIR=./classes

test -d "$DIST_DIR" || mkdir "$DIST_DIR"

mkdir $CLASSES_DIR

echo $CLASSPATH

javac -sourcepath $SRC_DIR $CLASSPATH -d $CLASSES_DIR -g $SRC_DIR/us/kbase/genomecomparison/*.java $SRC_DIR/us/kbase/genomecomparison/test/*.java
cp -r $SRC_DIR/us/kbase/genomecomparison/gbk/qualifier_types.properties $CLASSES_DIR/us/kbase/genomecomparison/gbk/

java -classpath $JARS:$CLASSES_DIR org.junit.runner.JUnitCore us.kbase.genomecomparison.test.TaskHolderTest