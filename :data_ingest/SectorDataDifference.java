import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Scanner;
import java.util.regex.Pattern;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SectorDataDifference{

 
    public static class SectorMapper extends Mapper<Object, Text, Text, Text> {
        private Text sectorName = new Text();
        private Text yearSum = new Text();


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] lineStore = line.split(",");
            boolean check = true;
            for(int i =0; i <lineStore.length;i++){
                if(lineStore[i].equals("")){
                    check = false;
                    break;
                }
            }

        if (lineStore.length >= 13 && !lineStore[0].contains("NAICS")) {
            if(check){
            ArrayList<String> newStore = new ArrayList<String>();
            Pattern pattern = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

            try (Scanner scanner = new Scanner(line).useDelimiter(pattern)) {
                while (scanner.hasNext()) {
                    String element = scanner.next().replace(",", "").replaceAll("\"", "").trim();
                    newStore.add(element);
                }
            }

            String sector = newStore.get(0);
            double sum = 0;
            
            for (int i = 2; i < newStore.size()-1; i++) {
                double a = Double.parseDouble(newStore.get(i));
                sum +=a;}
            int year = (int) Double.parseDouble(newStore.get(newStore.size() - 1).replace(",", "").replaceAll("\"", "").trim());


            sectorName.set(sector);
            yearSum.set(year + "," + sum);
            context.write(sectorName, yearSum);}
            
            
        }}
    }

   public static class SectorReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<Double> data = new ArrayList<>();
        for (Text val : values) {
            String[] fields = val.toString().split(",");
            double sum = Double.parseDouble(fields[1]);
            data.add(sum);
        }

        List<Double> differences = new ArrayList<>();
        for (int i = 1; i < data.size(); i++) {
            double difference = data.get(i) - data.get(i - 1);
            differences.add(difference);
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < differences.size(); i++) {
            BigDecimal inputting = new BigDecimal(differences.get(i));
            BigDecimal a = inputting.setScale(2, RoundingMode.HALF_UP);
            if (i != differences.size() - 1) {
                sb.append(a).append(",");
            } else {
                sb.append(a);
            }
        }
        result.set(sb.toString());
        context.write(key, result);}}


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // We want the cleaning or the analytic data output to be readable and easy to handle
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "Sector Data Difference");
        job.setJarByClass(SectorDataDifference.class);
        job.setMapperClass(SectorMapper.class);
        job.setReducerClass(SectorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setNumReduceTasks(1);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1); // Added missing parenthesis and ternary expression
}

}