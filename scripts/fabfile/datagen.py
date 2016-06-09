from fabric.api import run,cd,parallel,serial
from fabric.contrib.files import exists
counter = 0

@parallel
def tpch_remove():
    if exists('/data/mdindex/'):
        run('rm -rf /data/mdindex/')

@parallel
def download_tpch_dbgen():
    if not exists('/data/mdindex/'):
        run('mkdir /data/mdindex/')

    if not exists('/data/mdindex/tpch-dbgen'):
        with cd('/data/mdindex/'):
            run('git clone https://github.com/electrum/tpch-dbgen.git')

        with cd('/data/mdindex/tpch-dbgen'):
            run('make')

    with cd('/data/mdindex/tpch-dbgen'):
        try:
            run('rm -f *.tbl.*')
            run('rm -f *.tbl')
        except:
            pass

@serial
def tpch_script_gen_100():
    global counter
    with cd('/data/mdindex/'):
        run('rm -rf tpch100')
        run('mkdir tpch100')

    with cd('/data/mdindex/tpch-dbgen'):
        script = "#!/bin/bash\n"
        script += "cd /data/mdindex/tpch-dbgen/\n"
        script += './dbgen -s 100 -C 10 -S %d\n' % (counter + 1)
        script += 'mv *.tbl.* ../tpch100/\n'
        script += 'mv *.tbl ../tpch100/\n'

        run('echo "%s" > data_gen.sh' % script)
        run('chmod +x data_gen.sh')

    counter += 1

@parallel
def tpch_gen_100():
    run('nohup /data/mdindex/tpch-dbgen/data_gen.sh >> /tmp/xxx 2>&1 < /dev/null &', pty=False)

@serial
def script_move_data_tpch():
    # Create directory on HDFS first.
    global counter
    with cd('/data/mdindex/tpch100/'):
        script = "#!/bin/bash\n"
        script += "/home/mdindex/hadoop-2.6.0/bin/hadoop fs -copyFromLocal " + \
            "/data/mdindex/tpch100/*.tbl.%d /user/mdindex/tpch100-raw/\n" % (counter + 1)
        script += "/home/mdindex/hadoop-2.6.0/bin/hadoop fs -copyFromLocal " + \
            "/data/mdindex/tpch100/*.tbl /user/mdindex/tpch100-raw/"
        run('echo "%s" > move_data.sh' % script)
        run('chmod +x move_data.sh')

    counter += 1

@parallel
def move_data_tpch():
    run('nohup /data/mdindex/tpch100/move_data.sh >> /tmp/xxx 2>&1 < /dev/null &', pty=False)

# Next run
# ./spark-shell --master spark://128.30.77.86:7077 --packages com.databricks:spark-csv_2.11:1.2.0 --driver-memory 4G --executor-memory 100G -i <path to>/tpch.scala

@serial
def create_script_download_data():
    if exists('/data/mdindex/tpchd100'):
        run('rm -rf /data/mdindex/tpchd100')

    run('mkdir /data/mdindex/tpchd100')

    global counter
    with cd('/data/mdindex/tpchd100'):
        script = "#!/bin/bash\n"
        script += "/home/mdindex/hadoop-2.6.0/bin/hadoop fs -get " + \
            "/user/mdindex/tpch100/part-*%d /data/mdindex/tpchd100\n" % counter
        run('echo "%s" > download_data.sh' % script)
        run('chmod +x download_data.sh')

    counter += 1

@parallel
def download_data():
    run('nohup /data/mdindex/tpchd100/download_data.sh >> /tmp/xxx 2>&1 < /dev/null &', pty=False)


@serial
def script_move_data_cmt():
    # Create directory on HDFS first.
    with cd('/data/mdindex/yilu/cmt100000000'):
        script = "#!/bin/bash\n"
        script += "/home/mdindex/hadoop-2.6.0/bin/hadoop fs -copyFromLocal " + \
            "/data/mdindex/yilu/cmt100000000/*.txt.* /user/mdindex/cmt100-raw/\n"
        run('echo "%s" > move_data.sh' % script)
        run('chmod +x move_data.sh')

@parallel
def move_data_cmt():
    run('nohup /data/mdindex/yilu/cmt100000000/move_data.sh >> /tmp/xxx 2>&1 < /dev/null &', pty=False)

# Next run
# ./spark-shell --master spark://128.30.77.86:7077 --packages com.databricks:spark-csv_2.11:1.2.0 --driver-memory 4G --executor-memory 100G -i <path to>/cmt.scala

@serial
def script_download_data_cmt():
    global counter
    run('rm -rf /data/mdindex/cmtd100')
    run('mkdir /data/mdindex/cmtd100')

    with cd('/data/mdindex/cmtd100'):
        script = "#!/bin/bash\n"
        script += "/home/mdindex/hadoop-2.6.0/bin/hadoop fs -get " + \
            "/user/mdindex/cmt100/part-*%d /data/mdindex/cmtd100/\n" % counter
        run('echo "%s" > download_data.sh' % script)
        run('chmod +x download_data.sh')

    counter += 1

@parallel
def download_data_cmt():
    run('nohup /data/mdindex/cmtd100/download_data.sh >> /tmp/xxx 2>&1 < /dev/null &', pty=False)

@parallel
def create_script_stop_dbgen():
    with cd('/data/mdindex/yilu'):
        script = "#!/bin/bash\n"
        script += "pkill dbgen"
        run('echo "%s" > stop_dbgen.sh' % script)
        run('chmod +x stop_dbgen.sh')

@parallel
def stop_dbgen():
    run('nohup /data/mdindex/yilu/stop_dbgen.sh >> /tmp/xxx 2>&1 < /dev/null &', pty=False)

@parallel
def create_script_clear_tmp():
    if not exists('/data/mdindex/yilu'):
        run('mkdir /data/mdindex/yilu')
    with cd('/data/mdindex/yilu'):
        script = "#!/bin/bash\n"
        script += "rm -rf /tmp/*"
        script += "rm -rf /tmp/xxx"
        run('echo "%s" > clear_tmp.sh' % script)
        run('chmod +x clear_tmp.sh')

@parallel
def clear_tmp():
    run('nohup /data/mdindex/yilu/clear_tmp.sh >> /tmp/xxx 2>&1 < /dev/null &', pty=False)

@serial
def create_script_download_data_join():
    if exists('/data/mdindex/yilu/tpch100'):
        run('rm -rf /data/mdindex/yilu/tpch100')

    run('mkdir /data/mdindex/yilu/tpch100')

    if not exists('/data/mdindex/yilu/tpch100/lineitem'):
        run('mkdir /data/mdindex/yilu/tpch100/lineitem')
    if not exists('/data/mdindex/yilu/tpch100/customer'):
        run('mkdir /data/mdindex/yilu/tpch100/customer')
    if not exists('/data/mdindex/yilu/tpch100/part'):
        run('mkdir /data/mdindex/yilu/tpch100/part')
    if not exists('/data/mdindex/yilu/tpch100/orders'):
        run('mkdir /data/mdindex/yilu/tpch100/orders')
    if not exists('/data/mdindex/yilu/tpch100/supplier'):
        run('mkdir /data/mdindex/yilu/tpch100/supplier')


    global counter
    with cd('/data/mdindex/yilu'):
        script = "#!/bin/bash\n"
        script += "/home/mdindex/hadoop-2.6.0/bin/hadoop fs -get " + \
            "/user/yilu/tpch100-spark/lineitem/part-*%d /data/mdindex/yilu/tpch100/lineitem\n" % counter
        script += "/home/mdindex/hadoop-2.6.0/bin/hadoop fs -get " + \
            "/user/yilu/tpch100-spark/customer/part-*%d /data/mdindex/yilu/tpch100/customer\n" % counter
        script += "/home/mdindex/hadoop-2.6.0/bin/hadoop fs -get " + \
            "/user/yilu/tpch100-spark/part/part-*%d /data/mdindex/yilu/tpch100/part\n" % counter
        script += "/home/mdindex/hadoop-2.6.0/bin/hadoop fs -get " + \
            "/user/yilu/tpch100-spark/orders/part-*%d /data/mdindex/yilu/tpch100/orders\n" % counter
        script += "/home/mdindex/hadoop-2.6.0/bin/hadoop fs -get " + \
            "/user/yilu/tpch100-spark/supplier/part-*%d /data/mdindex/yilu/tpch100/supplier\n" % counter
        run('echo "%s" > download_data.sh' % script)
        run('chmod +x download_data.sh')

    counter += 1

@parallel
def download_data_join():
    run('nohup /data/mdindex/yilu/download_data.sh >> /tmp/xxx 2>&1 < /dev/null &', pty=False)


