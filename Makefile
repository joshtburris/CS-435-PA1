TESTFILE=/PA1/CS435PA1Dataset
#TESTFILE=/PA1/pa1testfile
RESULT=/PA1/result
LOCAL_RESULT=~/cs435/PA1/result
HGET=${HADOOP_HOME}/bin/hadoop fs -get
HJAR=${HADOOP_HOME}/bin/hadoop jar PA1.jar PA1
HREMOVE=-${HADOOP_HOME}/bin/hadoop fs -rm -R

build:
	${HADOOP_HOME}/bin/hadoop com.sun.tools.javac.Main PA1.java
	jar cf ~/cs435_workspace/PA1/src/PA1.jar PA1 *.class

profile1: clean
	${HJAR} profile1 ${TESTFILE} ${RESULT}
	${HGET} ${RESULT} ${LOCAL_RESULT}

profile2: clean
	 ${HJAR} profile2 ${TESTFILE} ${RESULT}
	 ${HGET} ${RESULT} ${LOCAL_RESULT}

profile3: clean
	${HJAR} profile3 ${TESTFILE} ${RESULT}
	${HGET} ${RESULT} ${LOCAL_RESULT}

clean:
	-rm -R ${LOCAL_RESULT}
	${HREMOVE} ${RESULT}
	${HREMOVE} /user/joshtb/job1Output
