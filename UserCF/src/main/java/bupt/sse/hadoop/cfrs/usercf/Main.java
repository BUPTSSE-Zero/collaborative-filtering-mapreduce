package bupt.sse.hadoop.cfrs.usercf;

import bupt.sse.hadoop.cfrs.usercf.job.Step1MovieRatingMatrixJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Shengyun Zhou <GGGZ-1101-28@Live.cn>
 */
public class Main {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        /*User CF*/
        //Step 1
        Job userCFjob1 = Job.getInstance(conf, "UserCFStep1");
        userCFjob1.setJarByClass(Main.class);
        userCFjob1.setMapperClass(Step1MovieRatingMatrixJob.JobMapper.class);
        userCFjob1.setReducerClass(Step1MovieRatingMatrixJob.JobReducer.class);
        userCFjob1.setOutputKeyClass(IntWritable.class);
        userCFjob1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(userCFjob1, new Path(args[0]));
        FileOutputFormat.setOutputPath(userCFjob1, new Path(args[1]));
        userCFjob1.waitForCompletion(true);
    }
}
