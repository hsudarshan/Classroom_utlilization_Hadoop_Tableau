import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class q4 {
    public static final String customDelimiter = "~";

    public static boolean cleanData(String str) {
        String[] fields = str.split(",");
        if (fields.length > 11) {
            return false;
        }
        if (fields[5].toString().trim().toUpperCase().startsWith("UNK") || fields[5].toString().trim().toUpperCase().startsWith("ARR")) {
            return false;
        }
        if (fields[6].toString().trim().toUpperCase().startsWith("UNK") || fields[6].toString().trim().toUpperCase().startsWith("ARR")) {
            return false;
        }
        if (fields[7].toString().trim().toUpperCase().startsWith("UNK") || fields[7].toString().trim().toUpperCase().startsWith("ARR")) {
            return false;
        }
        if (Integer.parseInt(fields[9].toString().trim()) < 0) {
            return false;
        }
        return true;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance((Configuration)conf, (String)"data elimination and count into ranges");
        job.setJarByClass((Class)q4.class);
        job.setMapperClass((Class)Stage1Mapper.class);
        job.setReducerClass((Class)Stage1Reducer.class);
        job.setOutputKeyClass((Class)Text.class);
        job.setOutputValueClass((Class)IntWritable.class);
        FileInputFormat.addInputPath((Job)job, (Path)new Path(args[1]));
        FileOutputFormat.setOutputPath((Job)job, (Path)new Path(args[2]));
        job.waitForCompletion(true);
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance((Configuration)conf2, (String)"percentile for dept ranges");
        job2.setJarByClass((Class)q4.class);
        job2.setMapperClass((Class)Stage2Mapper.class);
        job2.setMapOutputValueClass((Class)Text.class);
        job2.setReducerClass((Class)Stage2Reducer.class);
        job2.setOutputKeyClass((Class)Text.class);
        job2.setOutputValueClass((Class)Text.class);
        FileInputFormat.addInputPath((Job)job2, (Path)new Path(args[2]));
        FileOutputFormat.setOutputPath((Job)job2, (Path)new Path(args[3]));
        job2.waitForCompletion(true);
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance((Configuration)conf2, (String)"sort based on dept and percentile");
        job3.setJarByClass((Class)q4.class);
        job3.setMapperClass((Class)Stage3Mapper.class);
        job3.setMapOutputValueClass((Class)Text.class);
        job3.setReducerClass((Class)Stage3Reducer.class);
        job3.setOutputKeyClass((Class)Text.class);
        job3.setOutputValueClass((Class)Text.class);
        FileInputFormat.addInputPath((Job)job3, (Path)new Path(args[3]));
        FileOutputFormat.setOutputPath((Job)job3, (Path)new Path(args[4]));
        job3.waitForCompletion(true);
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }

    public static class Stage1Mapper
    extends Mapper<Object, Text, Text, IntWritable> {
        private IntWritable mapValue = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable> context) throws IOException, InterruptedException {
            try {
                String input = value.toString();
                boolean flag = q4.cleanData(input);
                if (!flag) {
                    return;
                }
                String[] fields = input.split(",");
                String dept = fields[4];
                String location = fields[5];
                int curStudents = Integer.parseInt(fields[9]);
                String[] loc = location.toString().split(" ");
                if (loc.length == 1) {
                    return;
                }
                if (loc[1].trim().toUpperCase().equals("ARR") || loc[1].trim() == "") {
                    return;
                }
                String range = new String();
                if (curStudents < 0) {
                    return;
                }
                range = curStudents > 0 && curStudents < 20 ? "000-019" : (curStudents >= 20 && curStudents < 60 ? "020-059" : (curStudents >= 60 && curStudents < 100 ? "060-099" : (curStudents >= 100 && curStudents < 140 ? "100-139" : (curStudents >= 140 && curStudents < 180 ? "140-179" : (curStudents >= 180 && curStudents < 220 ? "180-219" : "220-Inf")))));
                StringBuilder sb = new StringBuilder();
                sb.append(dept).append("~").append(range);
                this.word.set(sb.toString());
                context.write((Object)this.word, (Object)this.mapValue);
            }
            catch (NumberFormatException input) {
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class Stage1Reducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable reducerValue = new IntWritable();
        private Text reducerKey = new Text();

        public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable> context2) throws IOException, InterruptedException {
            String dept = key.toString();
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            this.reducerKey.set(key);
            this.reducerValue.set(sum);
            context2.write((Object)this.reducerKey, (Object)this.reducerValue);
        }
    }

    public static class Stage2Mapper
    extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text Mappervalue = new Text();

        public void map(Object key, Text value, Mapper<Object, Text, Text, Text> context) throws IOException, InterruptedException {
            try {
                String[] fields = value.toString().split("\\t");
                String[] ipkey = fields[0].toString().split("~");
                StringBuilder sb = new StringBuilder();
                sb.append(ipkey[1]).append("~").append(fields[1]);
                this.word.set(ipkey[0]);
                this.Mappervalue.set(sb.toString());
                context.write((Object)this.word, (Object)this.Mappervalue);
            }
            catch (NumberFormatException fields) {
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class Stage2Reducer
    extends Reducer<Text, Text, Text, Text> {
        private Text reducerValue = new Text();
        private Text reducerKey = new Text();

        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text> context2) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            String dept = key.toString();
            ArrayList<String> al = new ArrayList<String>();
            for (Text val : values) {
                al.add(val.toString());
                int ipCnt = Integer.parseInt(val.toString().split("~")[1]);
                sum += ipCnt;
                ++count;
            }
            if (sum == 0) {
                ++sum;
            }
            int i = 0;
            while (i < al.size()) {
                String[] rangeCount = ((String)al.get(i)).split("~");
                int cntPercentile = Integer.parseInt(rangeCount[1]) * 100 / sum;
                StringBuilder sb = new StringBuilder();
                sb.append(dept).append("~").append(String.format("%03d", cntPercentile));
                this.reducerKey.set(sb.toString());
                this.reducerValue.set(rangeCount[0]);
                context2.write((Object)this.reducerKey, (Object)this.reducerValue);
                ++i;
            }
        }
    }

    public static class Stage3Mapper
    extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text Mappervalue = new Text();

        public void map(Object key, Text value, Mapper<Object, Text, Text, Text> context) throws IOException, InterruptedException {
            try {
                String[] fields = value.toString().split("\\t");
                this.word.set(fields[0].toString());
                this.Mappervalue.set(fields[1].toString());
                context.write((Object)this.word, (Object)this.Mappervalue);
            }
            catch (NumberFormatException fields) {
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class Stage3Reducer
    extends Reducer<Text, Text, Text, Text> {
        private Text reducerValue = new Text();
        private Text reducerKey = new Text();

        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text> context2) throws IOException, InterruptedException {
            Iterator<Text> iter = values.iterator();
            String val = iter.next().toString();
            this.reducerKey.set(key);
            this.reducerValue.set(val);
            context2.write((Object)this.reducerKey, (Object)this.reducerValue);
        }
    }

}

