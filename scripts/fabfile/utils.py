from fabric.api import *
from fabric.contrib.files import exists
import fabric

counter = 0

@roles('master')
def start_spark():
    run('/home/mdindex/scripts/startSystems.sh')

@roles('master')
def stop_spark():
    run('/home/mdindex/scripts/stopSystems.sh')

@roles('master')
def start_zookeeper():
    run('/home/mdindex/scripts/startZookeeper.sh')

@roles('master')
def stop_zookeeper():
    run('/home/mdindex/scripts/stopZookeeper.sh')

def run_bg(cmd, before=None, sockname="dtach", use_sudo=False):
    """Run a command in the background using dtach

    :param cmd: The command to run
    :param output_file: The file to send all of the output to.
    :param before: The command to run before the dtach. E.g. exporting
                   environment variable
    :param sockname: The socket name to use for the temp file
    :param use_sudo: Whether or not to use sudo
    """
    if not exists("/usr/bin/dtach"):
        print "Install dtach first !"
        return
    if before:
        cmd = "{}; dtach -n `mktemp -u /tmp/{}.XXXX` {}".format(
            before, sockname, cmd)
    else:
        cmd = "dtach -n `mktemp -u /tmp/{}.XXXX` {}".format(sockname, cmd)
    if use_sudo:
        return sudo(cmd)
    else:
        return run(cmd)

@runs_once
def build_jar():
    local('cd /Users/anil/Dev/repos/mdindex/; gradle shadowJar')

@parallel
def update_jar():
    if not exists('/data/mdindex/jars'):
        run('mkdir -p /data/mdindex/jars')
    put('../build/libs/amoeba-all.jar', '/data/mdindex/jars/')

@roles('master')
def update_master_jar():
    if not exists('/data/mdindex/jars'):
        run('mkdir -p /data/mdindex/jars')
    put('../build/libs/amoeba-all.jar', '/data/mdindex/jars/')

@serial
def update_config():
    global counter
    put('server/server.properties', '/home/mdindex/amoeba.properties')
    run('echo "MACHINE_ID = %d" >> /home/mdindex/amoeba.properties' % counter)
    counter += 1

@parallel
def clean_cluster():
  run('rm -R /data/mdindex/logs/hadoop/')
  run('rm -R /home/mdindex/spark-1.6.0-bin-hadoop2.6/logs/')
  run('rm -R /home/mdindex/spark-1.6.0-bin-hadoop2.6/work/')

@parallel
def parallel_shell():
  run('mv /data/mdindex/tpchd100/download_data.sh ~/')
