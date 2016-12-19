#collaborative-filtering-mapreduce 

A realize of collaborative filtering recommendation system based on Hadoop.

## Dataset Source Reference

[http://grouplens.org/datasets/movielens](http://grouplens.org/datasets/movielens)

## Prerequisite

+ [Gradle](https://gradle.org/)

+ [Hadoop](http://hadoop.apache.org/)(2.x)

+ Recommended IDE: IntelliJ IDEA

NOTE: If you use Eclipse to develop, your must install [Buildship](https://github.com/eclipse/buildship/blob/master/docs/user/Installation.md) plugin to support Gradle.

## Configure Properties

Before building this project, you should configure some properties in `gradle.properties`:

+ `PROJECT_HADOOP_VERSION`: version of your installed Hadoop.

+ `HADOOP_HOME`: Hadoop installation path.

+ `PROJECT_HDFS_INPUT_DIR`: HDFS directory used for input.

+ `PROJECT_HDFS_OUTPUT_DIR`: HDFS diretory used for output.

## Project Structure

+ collaborative-filtering-mapreduce(root project)
  + ItemCF(sub project)

  + UserCF(sub project)

  + Shared(sub project): code shared by all other sub projects.


## Build JAR

To build, simply run

```shell
gradle shadowJar
```

Generated JAR will be built into:

+ ItemCF/build/libs	(for sub project `ItemCF`)
+ UserCF/build/libs (for sub project `UserCF`)

## Gradle Task

Some useful Gradle tasks:

For root project:

+ `shadowJar`: Build JAR for all sub projects with runtime dependencies.
+ `startHadoop`: Start HDFS and YARN.
+ `stopHadoop`: Stop HDFS and YARN.
+ `uploadDataFiles`: Upload all files in directory `DatasetFiles` to `PROJECT_HDFS_INPUT_DIR`.

For sub project `ItemCF`:

+ `shadowJar`: Build JAR for this sub project only. Generated JAR will be built into `ItemCF/build/libs`.
+ `buildAndRun`: Build JAR and run it in Hadoop environment.

For sub project `UserCF`:

+ `shadowJar`: Build JAR for this sub project only. Generated JAR will be built into `UserCF/build/libs`.
+ `buildAndRun`: Build JAR and run it in Hadoop environment.
