#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

# Set MPI-D specific environment variables here.

# The only required environment variable is JAVA_HOME.  All others are
# optional.  When running a distributed configuration it is best to
# set JAVA_HOME in this file, so that it is correctly defined on
# remote nodes.

# The java implementation to use.  Required.
# export JAVA_HOME=/usr/lib/j2sdk1.5-sun

JAVA_HEAP_MAX=-Xmx2048m

# Define the hostfile path, this parameter is needed when using OPENMPI
# One can unset this parameter and use "-f ${hostfile_path}" to reset the
# hostfile path in the command.
HOSTFILE_PATH=${MPI_D_CONF_PATH}/hostfile

# Define the Hadoop HDFS configuration directory path
HADOOP_CONF_PATH=/home/lf/workplace/BenchScripts/frameworks/hadoop-2.7.3/etc/hadoop

# If using hdfs v2, set "V2", else "V1"
export HDFS_VERSION="V2"