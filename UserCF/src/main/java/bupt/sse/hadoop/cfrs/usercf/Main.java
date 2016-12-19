package bupt.sse.hadoop.cfrs.usercf;

import bupt.sse.hadoop.cfrs.usercf.job.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author Shengyun Zhou <GGGZ-1101-28@Live.cn>
 */
public class Main {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(TextOutputFormat.SEPERATOR, "/");

        /*User CF*/
        //Step 1
        Job userCFJob1 = Job.getInstance(conf, "UserCFStep1");
        userCFJob1.setJarByClass(Main.class);
        userCFJob1.setMapperClass(Step1MovieRatingMatrixJob.JobMapper.class);
        userCFJob1.setReducerClass(Step1MovieRatingMatrixJob.JobReducer.class);
        userCFJob1.setOutputKeyClass(IntWritable.class);
        userCFJob1.setOutputValueClass(Text.class);
        userCFJob1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(userCFJob1, new Path(args[0]));
        FileOutputFormat.setOutputPath(userCFJob1, new Path(args[1]));
        userCFJob1.waitForCompletion(true);

        Job userCFJob2 = Job.getInstance(conf, "UserCFStep2");
        userCFJob2.setJarByClass(Main.class);
        userCFJob2.setMapperClass(Step2SimilarityMatrixJob.JobMapper.class);
        userCFJob2.setReducerClass(Step2SimilarityMatrixJob.JobReducer.class);
        userCFJob2.setOutputKeyClass(Text.class);
        userCFJob2.setOutputValueClass(DoubleWritable.class);
        userCFJob2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(userCFJob2, new Path(args[1]));
        FileOutputFormat.setOutputPath(userCFJob2, new Path(args[2]));
        userCFJob2.waitForCompletion(true);

        Job userCFJob3 = Job.getInstance(conf, "UserCFStep3");
        userCFJob3.setJarByClass(Main.class);
        userCFJob3.setMapperClass(Step3ConvertSimilarityMatrixJob.JobMapper.class);
        userCFJob3.setReducerClass(Step3ConvertSimilarityMatrixJob.JobReducer.class);
        userCFJob3.setMapOutputKeyClass(IntWritable.class);
        userCFJob3.setMapOutputValueClass(Text.class);
        userCFJob3.setOutputKeyClass(IntWritable.class);
        userCFJob3.setOutputValueClass(IntWritable.class);
        userCFJob3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(userCFJob3, new Path(args[2]));
        FileOutputFormat.setOutputPath(userCFJob3, new Path(args[3]));
        userCFJob3.waitForCompletion(true);

        Job userCFJob4 = Job.getInstance(conf, "UserCFStep4");
        userCFJob4.setJarByClass(Main.class);
        userCFJob4.setMapperClass(Step4RecommenderRatingMatrixJob.JobMapper.class);
        userCFJob4.setReducerClass(Step4RecommenderRatingMatrixJob.JobReducer.class);
        userCFJob4.setMapOutputKeyClass(IntWritable.class);
        userCFJob4.setMapOutputValueClass(Text.class);
        userCFJob4.setOutputKeyClass(Text.class);
        userCFJob4.setOutputValueClass(Text.class);
        userCFJob4.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(userCFJob4, new Path(args[0]));
        FileInputFormat.addInputPath(userCFJob4, new Path(args[3]));
        FileOutputFormat.setOutputPath(userCFJob4, new Path(args[4]));
        userCFJob4.waitForCompletion(true);

        Job userCFJob5 = Job.getInstance(conf, "UserCFStep5");
        userCFJob5.setJarByClass(Main.class);
        userCFJob5.setMapperClass(Step5RecommendMatrixJob.JobMapper.class);
        userCFJob5.setReducerClass(Step5RecommendMatrixJob.JobReducer.class);
        userCFJob5.setMapOutputKeyClass(IntWritable.class);
        userCFJob5.setMapOutputValueClass(Text.class);
        userCFJob5.setOutputKeyClass(IntWritable.class);
        userCFJob5.setOutputValueClass(Text.class);
        userCFJob5.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(userCFJob5, new Path(args[0]));
        FileInputFormat.addInputPath(userCFJob5, new Path(args[4]));
        FileOutputFormat.setOutputPath(userCFJob5, new Path(args[5]));
        userCFJob5.waitForCompletion(true);
    }
}
