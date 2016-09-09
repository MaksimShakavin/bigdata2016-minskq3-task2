package com.epam.bigdata.minskq3.task2;

import org.apache.hadoop.fs.Path;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Aliaksei_Neuski on 8/31/16.
 */
public class Constants {
    public static final String HDFS_ROOT_PATH = "hdfs://sandbox.hortonworks.com:8020";
    public static final String HSFS_TMP_PATH = "/tmp/sya/";
    public static final String HIVE_JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    public static final String HIVE_DEFAULT_DB_NAME = "jdbc:hive2://localhost:10000/default";
    public static final String HIVE_DEFAULT_TABLE_NAME = "products";

    public static final Path HDFS_MY_APP_JAR_PATH = new Path(Constants.HDFS_ROOT_PATH + "/apps/simple-yarn-app-1.1.0-jar-with-dependencies.jar");

    public static final List<String> STOP_WORDS = Arrays.asList("to", "in", "a", "for");


}
