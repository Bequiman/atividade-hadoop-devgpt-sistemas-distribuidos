import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ContadorRepoLanguage {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "contador repolanguage");

        job.setJarByClass(ContadorRepoLanguage.class);
        job.setMapperClass(ContadorRepoLanguageMapper.class);
        job.setReducerClass(ContadorRepoLanguageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("arquivo-entrada"));
        FileOutputFormat.setOutputPath(job, new Path("arquivo-saida"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
