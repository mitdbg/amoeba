from fabric.api import *
from env_setup import *

@roles('master')
def shell():
    global conf
    cmd = '$SPARKSUBMIT --class perf.tools.Shell --deploy-mode client --master spark://localhost:7077 $JAR ' + \
        ' --conf $CONF '
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
def print_unique_tpch_queries():
    cmd = '$SPARKSUBMIT --class perf.benchmark.TPCHWorkload --deploy-mode client --master spark://localhost:7077 $JAR ' + \
        ' --conf $CONF' + \
        ' --method 5 > ~/logs/tpch_unique_queries.log'
    cmd = fill_cmd(cmd)
    run(cmd)

@roles('master')
def print_single_attribute_queries():
    with cd(env.conf['HADOOPBIN']):
        cmd = '$SPARKSUBMIT --class perf.benchmark.SingleAttributeQueries --deploy-mode client --master spark://localhost:7077 $JAR ' + \
            ' --conf $CONF' + \
            ' --tableName $TABLENAME' + \
            ' --attributeMix l_shipdate,l_commitdate' + \
            ' --selectivity 0.1' + \
            ' --numQueriesPerAttribute 6' + \
            ' > ~/logs/single_attribute_queries_workload.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@roles('master')
def print_cmt_queries():
    with cd(env.conf['HADOOPBIN']):
        cmd = '$SPARKSUBMIT --class perf.benchmark.CMTWorkload --deploy-mode client --master spark://localhost:7077 $JAR ' + \
            ' --conf $CONF' + \
            ' > ~/logs/cmt_queries.log'
        cmd = fill_cmd(cmd)
        run(cmd)


