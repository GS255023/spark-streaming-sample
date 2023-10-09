# Submit Python code to SparkMaster

if [ $# -lt 1 ]
then
	echo "Usage: $0 <pyspark-job.py> [ executor-memory ]"
	echo "(specify memory in string format such as \"512M\" or \"2G\")"
	exit 1
fi
PYTHON_JOB=$1

if [ -z $2 ]
then
	EXEC_MEM="512M"
else
	EXEC_MEM=$2
fi

spark-submit  --num-executors 1 \
	     	  --executor-memory $EXEC_MEM --executor-cores 1 \
              --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0  \
              $PYTHON_JOB
