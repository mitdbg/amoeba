from fabric.api import *
from env_setup import *

@roles('master')
def shell():
    global conf
    cmd = '$SPARKSUBMIT --class perf.benchmark.Shell --deploy-mode client --master spark://localhost:7077 $JAR ' + \
        ' --conf $CONF '
    cmd = fill_cmd(cmd)
    run(cmd)

@roles('master')
def run_tpch_queries():
    with cd(env.conf['HADOOPBIN']):
        cmd = '$SPARKSUBMIT --class perf.benchmark.TPCHWorkload --deploy-mode client --master spark://localhost:7077 $JAR ' + \
            ' --adapt false' + \
            ' --conf $CONF' + \
            ' --numQueries 1' + \
            ' --method 1 > ~/logs/tpch_workload.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@roles('master')
def print_tpch_queries():
    print "Num Queries: "
    num_queries = int(raw_input())
    with cd(env.conf['HADOOPBIN']):
        cmd = '$SPARKSUBMIT --class perf.benchmark.TPCHWorkload --deploy-mode client --master spark://localhost:7077 $JAR ' + \
            ' --conf $CONF' + \
            ' --numQueries %d' % num_queries + \
            ' --method 2 > ~/logs/tpch_queries.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@roles('master')
def run_tpchsparkjoin():
    with cd(env.conf['HADOOPBIN']):
        submit_script_path = "/home/mdindex/spark-1.6.0-bin-hadoop2.6/bin/spark-submit"
        cmd = submit_script_path + ' --class perf.benchmark.TPCHSparkJoinWorkload --deploy-mode client --master spark://128.30.77.86:7077 $JAR ' + \
            ' --numQueries 10' + \
            ' --method 1' + \
            ' --conf $CONF  > ~/logs/join_workload.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@roles('master')
def run_cmtsparkjoin():
    with cd(env.conf['HADOOPBIN']):
        submit_script_path = "/home/mdindex/spark-1.6.0-bin-hadoop2.6/bin/spark-submit"
        cmd = submit_script_path + ' --class perf.benchmark.CMTSparkJoinWorkload --deploy-mode client --master spark://128.30.77.88:7077 $JAR ' + \
            ' --numQueries 10' + \
            ' --method 1' + \
            ' --conf $CONF  > ~/logs/join_workload.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@roles('master')
def run_tpchjoin():
    with cd(env.conf['HADOOPBIN']):
        submit_script_path = "/home/mdindex/spark-1.6.0-bin-hadoop2.6/bin/spark-submit"
        cmd = submit_script_path + ' --class perf.benchmark.TPCHJoinWorkload --deploy-mode client --master spark://128.30.77.86:7077 $JAR ' + \
            ' --schemaLineitem "$SCHEMALINEITEM"'  + \
            ' --schemaOrders "$SCHEMAORDERS"'  + \
            ' --schemaCustomer "$SCHEMACUSTOMER"' + \
            ' --schemaPart "$SCHEMAPART"' + \
            ' --schemaSupplier "$SCHEMASUPPLIER"' + \
            ' --sizeLineitem 100' + \
            ' --sizeCustomer 94' + \
            ' --sizeSupplier 82' + \
            ' --sizeOrders 64' + \
            ' --sizePart 90' + \
            ' --numQueries 10' + \
            ' --method 1' + \
            ' --conf $CONF  > ~/logs/join_workload.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@roles('master')
def run_cmtjoin():
    with cd(env.conf['HADOOPBIN']):
        submit_script_path = "/home/mdindex/spark-1.6.0-bin-hadoop2.6/bin/spark-submit"
        cmd = submit_script_path + ' --class perf.benchmark.CMTJoinWorkload --deploy-mode client --master spark://128.30.77.88:7077 $JAR ' + \
            ' --schemaMH  "$SCHEMAMH"'  + \
            ' --schemaMHL "$SCHEMAMHL"'  + \
            ' --schemaSF  "$SCHEMASF"' + \
            ' --sizeMH 1210' + \
            ' --sizeMHL 12' + \
            ' --sizeSF 404' + \
            ' --method 1' + \
            ' --conf $CONF  > ~/logs/join_workload.log'
        cmd = fill_cmd(cmd)
        run(cmd)


@roles('master')
def run_cmt_queries():
    with cd(env.conf['HADOOPBIN']):
        cmd = '$SPARKSUBMIT --class perf.benchmark.CMTWorkload --deploy-mode client --master spark://localhost:7077 $JAR ' + \
            ' --conf $CONF' + \
            ' --schema "$SCHEMA"'  + \
            ' --numFields $NUMFIELDS' + \
            ' --numTuples $NUMTUPLES' + \
            ' --method 1 > ~/logs/cmt_workload.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@roles('master')
def test_cmt_queries():
    with cd(env.conf['HADOOPBIN']):
        cmd = '$SPARKSUBMIT --class perf.benchmark.CMTWorkload --deploy-mode client --master spark://localhost:7077 $JAR ' + \
            ' --conf $CONF' + \
            ' --schema "$SCHEMA"'  + \
            ' --numFields $NUMFIELDS' + \
            ' --numTuples $NUMTUPLES' + \
            ' --method 2 > ~/logs/cmt_workload_test.log'
        cmd = fill_cmd(cmd)
        run(cmd)

