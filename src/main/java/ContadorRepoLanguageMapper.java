import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class ContadorRepoLanguageMapper extends Mapper<Object, Text, Text, IntWritable> {   ;
    private final Text text = new Text();
    private final IntWritable intWritable = new IntWritable(1);

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tk = new StringTokenizer(value.toString());
        while (tk.hasMoreTokens()) {
            String token = tk.nextToken();
            if (token.contains("\"RepoLanguage\"")){
                token = tk.nextToken();
                System.out.println(token);
                text.set(token.toLowerCase().replaceAll("[\\W]",""));
                context.write(text, intWritable);
            }
        }
    }
}
