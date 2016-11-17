# Readme file for setting up Apache Mesos support for IBM® InfoSphere Streams
This Readme file describes how to install and configure Apache Mesos for InfoSphere® Streams.

Brian M Williams, IBM
bmwilli@us.ibm.com

# UNDER CONSTRUCTION
Please understand that code and this README are at a point in time of ongoing initial development.
See DEVELOP.txt file for notes on what needs to be done

# Design
The current implementation was inspired by the book <b>"Building Applications on Mesos"</b>

<b>Chapter 4 - Creating a new Framework for Mesos</b> was used as a template and reference
<br>
<b>Chapter 5 - Building a Mesos Executor</b> up to the point where it stated "YOU PROBABLY DON'T WANT TO DO THIS".  

## Mesos Approach to Resource Management
There is often questions and confusion with reagrds to Mesos being a resource manager that makes offers, rather than accepting requests (e.g. YARN).  In most cases (unless there are no resources left available) Mesos makes 
offers every second.  This is fast enough for the Streams Mesos Resource Manager to handle synchronous resource 
requests from Streams.

## A Tale of Two Threads - ResourceManagerAdapter and Mesos Scheduler
There are two primary threads running in this Resource Manager.  The main class, StreamsMesosResourceManager,
extends the Streams ResourceManagerAdapter class and listens for allocation and release requests from the StreamsResourceServer (launched by the streams-on-mesos script).
Simultaneously, the StreamsMesosScheduler class is running in a thread and listening for resource offers and
other messages from Mesos.

Between these to classes sits the shared state of the StreamsMesosState and the collection of StreamsMesosResource objects.

The StreamsMesosResourceManager creates new StreamsMesosResource objects in the state and then waits for a period of time checking to see if the requests are RUNNNING before reporting back to Streams the request is ALLOCATED or PENDING.

The StreamsMesosScheduler receives offers and if there are outstanding requests available, it determines if one of the offers is large enough for the request, and if so, launches the task (starts the streams controller in a task) using the built-in Mesos CommandExecutor.

## Mesos Executor - CommandExecutor
The initial design and implementation uses the built-in CommandExecutor of Mesos for running the getStartControllerCommand(... fork=false...) returned command.  By setting fork=false Mesos
keeps the parent process opened and shown as RUNNING in the Mesos web GUI.

When a resource is released, the ResourcemanagerUtilities.stopController() command is executed and the shutdown of the controller is completed and Mesos marks the task as FINISHED.

# A few commands:

## mkdomain command
streamtool mkdomain --property domain.externalResourceManager=mesos

## start mesos manager 
./streams-on-mesos start --master zk://172.31.29.41:2181/mesos

## Get status of Mesos Resource Manager
./streams-on-mesos status -d StreamsDomain


## Dependencies
* Apache Maven
    * To check whether Maven is installed on your system, enter which mvn.
    * To download Maven, go to the [Apache Maven website](https://maven.apache.org/).
* InfoSphere Streams
* (optional) [Apache Hadoop Version 2.7.0](https://hadoop.apache.org/) for HDFS provisioning

## Compiling the application
Use Maven to compile the source code for the application in the InfoSphere Streams MESOS package.

1. Run the following command in your project directory. This command creates a tar.gz file in the target directory.

    `mvn package`
2. Extract the contents of the tar.gz file into a directory of your choice, change to that directory, and run the application.


## Testing on Linux Workstation
I have not tested these instructions yet

1. Build and Install Mesos (I used 1.0.1)

	See Mesos instructions

	export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/local/lib"

2. Run standalone Mesos and Slave (no zookeeper)

	mesos-master --ip=127.0.0.1 --work_dir=/var/lib/mesos
	mesos-agent --master=127.0.0.1:5050 --work_dir=/var/lib/mesos


3. Run zookeeper (from docker) (for Streams).

	docker run -d \
	-p 2181:2181 \
	-p 2888:2888 \
	-p 3888:3888 \
	jplock/zookeeper

	export STERAMS_ZKCONNECT=localhost:2181

4. Run streams-on-mesos using standalone master uri

	cd scripts
	./streams-on-mesos start --master localhost:5050 --deploy


## Helpful Notes

If you ever need to stop an inactive framework in mesos:

	curl -XPOST http://localhost:5050/master/teardown -d 'frameworkId=2fabdf51-36b0-4951-9138-719f56ba1ae7-0000'

## Testing on Workstation with Docker for development


0. Get IP Address of Docker Server

	export HOST_IP=127.0.0.1

1. Run zookeeper.

	docker run -d \
	> -p 2181:2181 \
	> -p 2888:2888 \
	> -p 3888:3888 \
	> garland/zookeeper

	export STERAMS_ZKCONNECT=localhost:2181

2. Ensure Streams variables set

	source <streams install location>/4.2.0.0/bin/streamsprofile.sh

3. Start Mesos Master

	docker run --net="host" \
	-p 5050:5050 \
	-e "MESOS_HOSTNAME=${HOST_IP}" \
	-e "MESOS_IP=${HOST_IP}" \
	-e "MESOS_ZK=zk://${HOST_IP}:2181/mesos" \
	-e "MESOS_PORT=5050" \
	-e "MESOS_LOG_DIR=/var/log/mesos" \
	-e "MESOS_QUORUM=1" \
	-e "MESOS_REGISTRY=in_memory" \
	-e "MESOS_WORK_DIR=/var/lib/mesos" \
	-d \
	garland/mesosphere-docker-mesos-master


## MORE TO COME ...
