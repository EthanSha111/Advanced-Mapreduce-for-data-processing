import java.io.IOException;
import java.util.Scanner;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.logging.Logger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountRecsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING = 9999;
    private static final Logger logger = Logger.getLogger(CountRecsMapper.class.getName());

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

       
        String line = value.toString();
        String[] lineStore = line.split(",");

        if (lineStore.length >= 9 && !lineStore[0].contains("NAICS")) {
            ArrayList<String> newStore = new ArrayList<String>();
            Pattern pattern = Pattern.compile("\t|,(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            int result = 0;
            try (Scanner scanner = new Scanner(line).useDelimiter(pattern)) {
                while (scanner.hasNext()) {

                    String element = scanner.next().replace(",", "").replaceAll("\"", "").trim();
                    
                    newStore.add(element);
                    result +=1;
                    
                }
            }
            String sector = newStore.get(0);
            
            context.write(new Text(sector), new IntWritable(result));
        }
    }
}
