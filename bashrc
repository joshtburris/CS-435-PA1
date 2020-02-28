# default configuration for interactive bash sessions
# modify as needed

# current hostname
export HOSTNAME="$(hostname)"

# setup system paths
export PATH="${HOME}/bin:/bin:/usr/bin:/sbin:/usr/sbin:/usr/local/bin:."
export MANPATH="/usr/share/man:/usr/local/man"
export LD_LIBRARY_PATH=""

# only owner permissions by default
umask 0077

# set default shell prompt
export PS1='\h:\w\$ '
export PS2='> '

# uncomment to use vi keybindings
#set -o vi

# check for terminal resize
shopt -s checkwinsize

# ignore up to 10 ^D's
export ignoreeof=10

# longer shell history
export HISTSIZE=1024

# uncomment and change preferred printer
# see ~info/printers
#export PRINTER=banjo

# use less to page
export PAGER='less'

# safe mode for rm, cp, and mv
# uncomment to ask for confirmation
#alias rm='rm -i'
#alias cp='cp -i'
#alias mv='mv -i'

# ls aliases
alias dir='ls -Alg'
alias la='ls -AlgF'
alias ll='ls -ltr'

# uncomment to use custom eclipse
#alias eclipse='/usr/local/bin/eclipse.sh'
alias intellij='/usr/local/idea/bin/idea.sh'

# set OS specific stuff
case `arch` in

  i686*)
    # 32-bit linux dependent commands go here
    ;;

  x86_64)
    # 64-bit linux dependent command go here
    ;;

  sun*)
    # SunOS specific commands go here
    ;;

  *)
    # set stuff for machines not listed above
    ;;

esac

# hadoop environment variables
export JAVA_HOME=/usr/local/jdk1.8.0_51-64
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_COMMON_HOME=${HADOOP_HOME}
export HADOOP_HDFS_HOME=${HADOOP_HOME}
export HADOOP_MAPRED_HOME=${HADOOP_HOME}
export YARN_HOME=${HADOOP_HOME}
export HADOOP_CONF_DIR=~/cs435/hadoopConf
export YARN_CONF_DIR=${HADOOP_CONF_DIR}
export HADOOP_LOG_DIR=/tmp/${USER}/hadoop-logs
export YARN_LOG_DIR=/tmp/${USER}/yarn-logs
export HADOOP_OPTS="-Dhadoop.tmp.dir=/s/${HOSTNAME}/a/tmp/hadoop-${USER}"
export HADOOP_COMMON_LIB_NATIVE_DIR=${HADOOP_HOME}/lib/native
export HADOOP_OPTS="-Djava.library.path=${HADOOP_HOME}/lib/native -Dhadoop.tmp.dir=/s/${HOSTNAME}/a/tmp/hadoop-${USER}"

alias hjavac="$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main"
alias hjar="$HADOOP_HOME/bin/hadoop jar"
alias hformat="$HADOOP_HOME/bin/hdfs namenode -format"
alias hput="$HADOOP_HOME/bin/hadoop fs -put"
alias hget="$HADOOP_HOME/bin/hadoop fs -get"
alias hleavesafemode="$HADOOP_HOME/bin/hadoop dfsadmin -safemode leave"
alias hadoop="$HADOOP_HOME/bin/hadoop"
alias start-dfs="$HADOOP_HOME/sbin/start-dfs.sh"
alias stop-dfs="$HADOOP_HOME/sbin/stop-dfs.sh"
alias start-yarn="$HADOOP_HOME/sbin/start-yarn.sh"
alias stop-yarn="$HADOOP_HOME/sbin/stop-yarn.sh"

# Jupyter notebook
export PATH=/usr/local/anaconda/bin:$PATH
export PATH=/usr/local/sage:$PATH

# tarball shortcut
alias tarball="tar -czvf"
