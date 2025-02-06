#!/usr/bin/env bash

# sudo apt install -y curl

#
# directory
#
cd
mkdir ~/dev
mkdir ~/ws
mkdir ~/data

mkdir ~/temp
mkdir ~/temp/logs
mkdir ~/temp/tmp
mkdir ~/temp/spark-events
mkdir ~/temp/warehouse

#
# python
#
# sudo add-apt-repository ppa:deadsnakes/ppa
# sudo apt update
# sudo apt install -y python3.11
# sudo update-alternatives --install /usr/bin/python python /usr/bin/python3.11 20
curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11
python -m pip install --upgrade pip
python -m pip install findspark
python -m pip install pycryptodomex
python -m pip install pyyaml
python -m pip install python-box
python -m pip install openpyxl
python -m pip install pyarrow
python -m pip install pandas
python -m pip install dask[complete]
python -m pip install loky
python -m pip install chardet
python -m pip install tqdm
python -m pip install python-dateutil
python -m pip install regex
python -m pip install build
python -m pip install pillow
python -m pip install pyjpegls
python -m pip install python-gdcm
python -m pip install pylibjpeg[all]

#
# jdk
#
cd ~/dev
jdkname=openjdk-17+35_linux-x64_bin.tar.gz
wget https://download.java.net/openjdk/jdk17/ri/$jdkname
tar xvzf $jdkname
rm jdk
ln -s jdk-17 jdk

echo "export JAVA_HOME=~/dev/jdk"  >>  ~/.bashrc

#
# hadoop
#
cd ~/dev
hadoopname=hadoop-3.4.1
wget https://dlcdn.apache.org/hadoop/common/$hadoopname/$hadoopname.tar.gz
tar xvzf $hadoopname.tar.gz $hadoopname/lib/native
rm hadoop
ln -s $hadoopname hadoop

echo "export HADOOP_HOME=~/dev/hadoop"  >>  ~/.bashrc
echo "export LD_LIBRARY_PATH=\$HADOOP_HOME/lib/native:\$LD_LIBRARY_PATH"  >>  ~/.bashrc

#
# spark
#
cd ~/dev
sparkname=spark-3.5.3
sparkname2=$sparkname-bin-hadoop3
wget https://dlcdn.apache.org/spark/$sparkname/$sparkname2.tgz
tar xzvf $sparkname2.tgz
rm spark
ln -s $sparkname2 spark

echo "export SPARK_HOME=~/dev/spark"  >>  ~/.bashrc
echo "export PYSPARK_PYTHON=python"  >>  ~/.bashrc
echo "export PATH=\$SPARK_HOME/bin:\$PATH"  >>  ~/.bashrc

sparkenv=~/dev/spark/conf/spark-env.sh
echo "#!/usr/bin/env bash"  >  $sparkenv
echo "SPARK_LOCAL_IP=127.0.0.1"  >>  $sparkenv
echo "SPARK_LOCAL_DIRS=~/temp/tmp"  >>  $sparkenv
echo "SPARK_LOG_DIR=~/temp/logs"  >>  $sparkenv
echo "SPARK_DRIVER_MEMORY=48g"  >> $sparkenv

sparkconf=~/dev/spark/conf/spark-defaults.conf
echo ""  >  $sparkconf
echo "spark.driver.maxResultSize                    16g"  >>  $sparkconf
echo "spark.eventLog.enabled                        true"  >>  $sparkconf
echo "spark.eventLog.dir                            /home/ups/temp/spark-events"  >>  $sparkconf
echo "spark.history.fs.logDirectory                 /home/ups/temp/spark-events"  >>  $sparkconf
echo "spark.sql.autoBroadcastJoinThreshold          -1"  >>  $sparkconf
echo "spark.sql.caseSensitive                       false"  >>  $sparkconf
echo "spark.sql.debug.maxToStringFields             2000"  >>  $sparkconf
echo "spark.sql.parquet.int96RebaseModeInRead       CORRECTED"  >>  $sparkconf
echo "spark.sql.parquet.int96RebaseModeInWrite      CORRECTED"  >>  $sparkconf
echo "spark.sql.warehouse.dir                       /home/ups/temp/warehouse"  >>  $sparkconf
