from fabric.api import run,put,cd,parallel,roles,serial,local,runs_once
from env_setup import *

@roles('master')
def simulator_adapt():
    global conf
    with cd(env.conf['HADOOPBIN']):
        cmd = './hadoop jar $JAR perf.tools.RunSimulator' + \
            ' --conf $CONF' + \
            ' --tableName $TABLENAME' + \
            ' --mode 1' + \
            ' --simName sim' + \
            ' --queriesFile ~/queries.log' + \
            ' > ~/logs/sim_adapt.log'
        cmd = fill_cmd(cmd)
        run(cmd)

@roles('master')
def simulator_noadapt():
    global conf
    with cd(env.conf['HADOOPBIN']):
        cmd = './hadoop jar $JAR perf.tools.RunSimulator' + \
            ' --conf $CONF' + \
            ' --tableName $TABLENAME' + \
            ' --mode 2' + \
            ' --simName sim' + \
            ' --queriesFile ~/queries.log' + \
            ' > ~/logs/sim_noadapt.log'
        cmd = fill_cmd(cmd)
        run(cmd)

