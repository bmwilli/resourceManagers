/**
 *
 */
package com.ibm.streams.resourcemgr.mesos;

/**
 * Constants for use by the Streams Mesos Resource Manager
 *
 * Many of these will migrate from constants to configuration parameters,
 * in which case, these will be default values.
 *
 * @author Brian M Williams
 *
 */
public class StreamsMesosConstants {

	/* Turn these into arguments */
	public static final String
		MESOS_MASTER = "zk://localhost:2181/mesos"
	;

	/* Identification Constants */
	public static final String
		RESOURCE_TYPE = "mesos",
		FRAMEWORK_NAME = "IBMStreamsRM"
	;

	/* Names of command line arguments */
	public static final String
		// url of mesos master e.g. (zk://host1:port1,host2:port2,.../mesos | localhost:5050)
		MESOS_MASTER_ARG = "--master",
		// streams zookeeer connect string (eg. "hosta:port1,hostb:port2,...")
		// may or may not be same zookeeper as mesos is using
		ZK_ARG = "--zkconnect",
		// Streams resource manager type (defaulting to RESOURCE_TYPE above)
		TYPE_ARG = "--type",
		// Java class name of manager
		MANAGER_ARG = "--manager",
		// location of streams installation if not deployed
		INSTALL_PATH_ARG = "--install-path",
		HOME_DIR_ARG = "--home-dir",
		// Flag to initiate deploying the streams resource package for tasks
		DEPLOY_ARG = "--deploy",
		PROP_FILE_ARG = "--properties",
		DOMAIN_ID_ARG = "--domain-id"
	;

	/* Streams provisioning constants */
	public static final String
		// Location to have Streams build resource package
		PROVISIONING_WORKDIR_PREFIX = "/tmp/streams.mesos",
		// Default location of location to stage resources for mesos to fetch
		// Needs to be accessible by all nodes (file://, hdfs://, http://)
		//PROVISIONING_SHARED_URI = "hdfs://streams_mesos_provision",
		PROVISIONING_SHARED_URI = "file://home/bmwilli/tmp",
		RES_STREAMS_BIN = "StreamsResourceInstall.tar",
		RES_STREAMS_BIN_NAME = "STREAMS_BIN"
	;

	/* Property file and properties */
	public static final String
		RM_PROPERTIES_FILE = "streams-mesos.properties"
	;
	// AM properties
	public static final String
		PROPS_DC_CORES="DC_CORES",
		PROPS_DC_MEMORY="DC_MEMORY",
		PROPS_WAIT_SYNC = "WAIT_SYNC_SECS",
		PROPS_WAIT_ASYNC = "WAIT_ASYNC_SECS",
		PROPS_WAIT_FLEXIBLE="WAIT_FLEXIBLE_SECS",
		PROPS_WAIT_HEARTBEAT="WAIT_HEARTBEAT_SECS"
	;
	/* Mesos resource allocation defaults */
	public static final double
		RM_MEMORY_DEFAULT = 2048,
		RM_CORES_DEFAULT = 1
	;

	/* Constants for specifying to use all resources in an offer */
	public static final double
		USE_ALL_CORES = -1,
		USE_ALL_MEMORY = -1
	;

	/* Streams tag names */
	public static final String
		MEMORY_TAG = "memory",
		CORES_TAG = "cores"
	;
	
	/* Polling interval, how long to sleep between polling checks */
	public static final long SLEEP_UNIT_MILLIS = 500;



}
