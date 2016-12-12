#collaborative-filtering-mapreduce 

A realize of collaborative filtering recommendation system based on Hadoop.

## Prerequisite

+ [Gradle](https://gradle.org/)

+ [Hadoop](http://hadoop.apache.org/)

+ Recommended IDE: IntelliJ IDEA

NOTE: If you use Eclipse to develop, your must install [Buildship](https://github.com/eclipse/buildship/blob/master/docs/user/Installation.md) plugin to support Gradle.

## Configure Properties

Before importing this project, you should configure some properties in `gradle.properties`:

+ `PROJECT_HADOOP_VERSION`: the version of your installed Hadoop.

+ `HADOOP_HOME`: Hadoop installation path.

+ `PROJECT_HDFS_INPUT_DIR`: HDFS directory used for input.

+ `PROJECT_HDFS_OUTPUT_DIR`: HDFS diretory used for output.

## Gradle Task

Some useful Gradle tasks:

+ `shadowJar`: Create a combined JAR of project and runtime dependencies.

+ `buildAndRun`: Create a runnable JAR by `shadowJar` task and run it in Hadoop environment. 

  NOTE: `PROJECT_HDFS_OUTPUT_DIR` will be deleted before every running.

+ `startHadoop`: Start HDFS and YARN.

+ `stopHadoop`: Stop HDFS and YARN.

+ `uploadDataFiles`: Upload all files in directory `DatasetFiles` to `PROJECT_HDFS_INPUT_DIR`.