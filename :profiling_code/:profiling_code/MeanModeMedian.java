import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import java.util.Collections;
import java.util.List;
import java.math.BigDecimal;
import java.math.RoundingMode;


public class MeanModeMedian{

 
    public static class SectorMapper extends Mapper<Object, Text, Text, Text> {
        private Text sectorName = new Text();
        private Text yearSum = new Text();


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] lineStore = line.split(",");
            boolean check = true;
            //I use the criteria that any null one in the row(which is rare in my table) should be eliminated for giving a precise 
            //report of the status quo of sectors
            for(int i =0; i <lineStore.length;i++){
                if(lineStore[i].equals("")){
                    check = false;
                    break;
                }
            }

        if (lineStore.length >= 13 && !lineStore[0].contains("NAICS")) {
            if(check){
            ArrayList<String> newStore = new ArrayList<String>();
            //Data cleaning to prevent the , in double influence the split result
            Pattern pattern = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

            try (Scanner scanner = new Scanner(line).useDelimiter(pattern)) {
                while (scanner.hasNext()) {
                    String element = scanner.next().replace(",", "").replaceAll("\"", "").trim();
                    newStore.add(element);
                }
            }
            //Data cleaning for better join
            String sector = newStore.get(0).toLowerCase();
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
        double meansum = 0;
        double mean = 0;
        for (Text val : values) {
            String[] fields = val.toString().split(",");
            double sum = Double.parseDouble(fields[1]);
            data.add(sum);
            meansum += sum;
        }
        mean = meansum / data.size();

        Collections.sort(data);
        double median;
        if (data.size() % 2 != 0) {
            median = data.get(data.size() / 2);
        } else {
            median = (data.get(data.size() / 2) + data.get(data.size() / 2 - 1)) / 2;
        }

        List<Double> mode = new ArrayList<>();
        int maxCount = 1;
        for (int i = 0; i < data.size(); i++) {
            int count = 1;
            for (int j = i + 1; j < data.size(); j++) {
                if (data.get(i).equals(data.get(j))) count++;
            }
            if (count >= maxCount) {
                if (count > maxCount) {
                    mode.clear();
                    maxCount = count;
                }
                mode.add(data.get(i));
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Mean: ").append(mean).append(",").append("Median: ").append(median).append(",Mode:");
        for (int i=0;i< mode.size();i++) {
            sb.append(data.get(i)).append(" ");
        }

        result.set(sb.toString());
        context.write(key, result);
    }
}


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sector Data Difference");
        job.setJarByClass(MeanModeMedian.class);
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

       