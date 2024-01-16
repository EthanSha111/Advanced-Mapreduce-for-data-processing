import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.Scanner;
import java.util.regex.Pattern;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

public class FindDistinct {
    public static class SectorMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text sectorName = new Text();
        private IntWritable result = new IntWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] lineStore = line.split(",");
            boolean check = true;

            for (int i = 0; i < lineStore.length; i++) {
                if (lineStore[i].equals("")) {
                    check = false;
                    break;
                }
            }

            if (lineStore.length >= 13 && !lineStore[0].contains("NAICS")) {
                if (check) {
                    ArrayList<String> newStore = new ArrayList<String>();
                    Pattern pattern = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

                    try (Scanner scanner = new Scanner(line).useDelimiter(pattern)) {
                        while (scanner.hasNext()) {
                            String element = scanner.next().replace(",", "").replaceAll("\"", "").trim();
                            newStore.add(element);
                        }
                    }
                    HashSet<String> hset = new HashSet<String>(newStore);
                    String sector = newStore.get(0).toLowerCase();

                    result.set(hset.size() - 1);
                    sectorName.set(sector);

                    context.write(sectorName, result);
                }
            }
        }
    }

    public static class SectorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int resultValue = 0;
            for (IntWritable value : values) {
                resultValue += value.get();
            }
            context.write(key, new IntWritable(resultValue));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FindDistinct");
        job.setJarByClass(FindDistinct.class);
        job.setMapperClass(SectorMapper.class);
        job.setReducerClass(SectorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setNumReduceTasks(1);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
