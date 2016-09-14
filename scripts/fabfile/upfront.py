from fabric.api import run,put,cd,parallel,roles,serial,local,runs_once
from env_setup import *

counter = 0

"""
Used to measure the time taken to simply upload
data into HDFS.
"""
@parallel
def hdfs_upload_test():
    global conf
    with cd(env.conf['HADOOPBIN']):
        cmd = './hadoop fs -copyFromLocal' + \
            ' $INPUTSDIR/*' + \
            ' $HDFSDIR/testUpload/'
        cmd = fill_cmd(cmd)
        run(cmd)

@roles('master')
def create_table_info():
    global conf
    with cd(env.conf['HADOOPBIN']):
        run('./hadoop fs -mkdir -p %s/%s' % (env.conf['HDFSDIR'], env.conf['TABLENAME']))
        cmd = './hadoop jar $JAR perf.tools.CreateTableInfo' + \
            ' --conf $CONF' + \
            ' --tableName $TABLENAME' + \
            ' --delimiter "$DELIMITER"' + \
            ' --schema "$SCHEMA"' + \
            ' --numTuples $NUMTUPLES' + \
            ' > ~/logs/create_info.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@parallel
def bulk_sample_gen():
    global conf
    run('mkdir -p %s/logs' % env.conf['HOMEDIR'])

    with cd(env.conf['HADOOPBIN']):
        cmd = './hadoop jar $JAR perf.tools.RunIndexBuilder' + \
            ' --conf $CONF' + \
            ' --tableName $TABLENAME' + \
            ' --inputsDir $INPUTSDIR' + \
            ' --method 1 ' + \
            ' --numReplicas 1' + \
            ' --samplingRate $SAMPLINGRATE' + \
            ' > ~/logs/sample_stats.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@roles('master')
def create_robust_tree():
    global conf
    print env.roledefs
    with cd(env.conf['HADOOPBIN']):
        cmd = './hadoop jar $JAR perf.tools.RunIndexBuilder' + \
            ' --conf $CONF' + \
            ' --tableName $TABLENAME' + \
            ' --inputsDir $INPUTSDIR' + \
            ' --method 2 ' + \
            ' --numBuckets $NUMBUCKETS' + \
            ' > ~/logs/create_tree.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@roles('master')
def create_kdtree():
    global conf
    print env.roledefs
    with cd(env.conf['HADOOPBIN']):
        cmd = './hadoop jar $JAR perf.tools.RunIndexBuilder' + \
            ' --conf $CONF' + \
            ' --tableName $TABLENAME' + \
            ' --inputsDir $INPUTSDIR' + \
            ' --method 8 ' + \
            ' --numBuckets $NUMBUCKETS' + \
            ' > ~/logs/create_tree.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@roles('master')
def create_range_tree():
    global conf
    print env.roledefs
    with cd(env.conf['HADOOPBIN']):
        cmd = './hadoop jar $JAR perf.tools.RunIndexBuilder' + \
            ' --conf $CONF' + \
            ' --tableName $TABLENAME' + \
            ' --inputsDir $INPUTSDIR' + \
            ' --method 9 ' + \
            ' --numBuckets $NUMBUCKETS' + \
            ' > ~/logs/create_tree.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@roles('master')
def create_hybrid_range_tree():
    global conf
    print env.roledefs
    with cd(env.conf['HADOOPBIN']):
        cmd = './hadoop jar $JAR perf.tools.RunIndexBuilder' + \
            ' --conf $CONF' + \
            ' --tableName $TABLENAME' + \
            ' --inputsDir $INPUTSDIR' + \
            ' --method 10 ' + \
            ' --numBuckets $NUMBUCKETS' + \
            ' > ~/logs/create_tree.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@roles('master')
def create_custom_tree():
    global conf
    print env.roledefs
    with cd(env.conf['HADOOPBIN']):
        cmd = './hadoop jar $JAR perf.tools.RunIndexBuilder' + \
            ' --conf $CONF' + \
            ' --tableName $TABLENAME' + \
            ' --inputsDir $INPUTSDIR' + \
            ' --method 11 ' + \
            ' --numBuckets $NUMBUCKETS' + \
            ' > ~/logs/create_tree.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@roles('master')
def create_join_robust_tree():
    global conf
    print env.roledefs
    with cd(env.conf['HADOOPBIN']):
        cmd = './hadoop jar $JAR perf.tools.RunIndexBuilder' + \
            ' --conf $CONF' + \
            ' --tableName $TABLENAME' + \
            ' --inputsDir $INPUTSDIR' + \
            ' --method 3 ' + \
            ' --numReplicas 1' + \
            ' --numBuckets $NUMBUCKETS' + \
            ' --joinAttribute $ATTRIBUTE' + \
            ' --joinAttributeDepth $DEPTH' + \
            ' > ~/logs/create_tree.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@parallel
def write_partitions():
    with cd(env.conf['HADOOPBIN']):
        cmd = './hadoop jar $JAR perf.tools.RunIndexBuilder' + \
            ' --conf $CONF' + \
            ' --tableName $TABLENAME' + \
            ' --inputsDir $INPUTSDIR' + \
            ' --method 4 ' + \
            ' --numReplicas 1' + \
            ' --numBuckets $NUMBUCKETS' + \
            ' > ~/logs/write_partitions.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@roles('master')
def write_partitions_spark():
    cmd = '$SPARKSUBMIT --class perf.tools.RunIndexBuilder --deploy-mode client --master spark://localhost:7077 $JAR ' + \
        ' --conf $CONF' + \
        ' --tableName $TABLENAME' + \
        ' --inputsDir $INPUTSDIR' + \
        ' --method 4 ' + \
        ' --numReplicas 1' + \
        ' --numBuckets $NUMBUCKETS' + \
        ' > ~/logs/write_partitions.log'
    cmd = fill_cmd(cmd)
    run(cmd)

@runs_once
def load_dataset():
  create_table_info()
  bulk_sample_gen()
  create_robust_tree()
  write_partitions()

@parallel
def write_join_partitions():
    with cd(env.conf['HADOOPBIN']):
        cmd = './hadoop jar $JAR perf.tools.RunIndexBuilder' + \
            ' --conf $CONF' + \
            ' --tableName $TABLENAME' + \
            ' --inputsDir $INPUTSDIR' + \
            ' --method 7 ' + \
            ' --numReplicas 1' + \
            ' --numBuckets $NUMBUCKETS' + \
            ' > ~/logs/write_partitions.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@roles('master')
def delete_partitions():
    with cd(env.conf['HADOOPBIN']):
        cmd = './hadoop fs -rm -R $HDFSDIR'
        cmd = fill_cmd(cmd)
        run(cmd)

@parallel
def check_max_memory():
    with cd(env.conf['HADOOPBIN']):
        cmd = './hadoop jar $JAR perf.tools.RunIndexBuilder ' + \
            ' --conf $CONF' + \
            ' --method 5 '
        cmd = fill_cmd(cmd)
        run(cmd)

#############
# Running Queries
############

@roles('master')
def upfront_spark_tpch_queries():
    cmd = '$SPARKSUBMIT --class perf.benchmark.baselines.SparkUpfront --deploy-mode client --master spark://localhost:7077 $JAR ' + \
        ' --conf $CONF' + \
        ' > ~/logs/spark_upfront.log'
    cmd = fill_cmd(cmd)
    run(cmd)

