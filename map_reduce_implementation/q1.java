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

public class q1 {
    public static final String customDelimiter = "~";

    public static boolean cleanData(String str) {
        String[] fields = str.split(",");
        if (fields.length > 9) {
            return false;
        }
        if (fields[2].toString().trim().toUpperCase().startsWith("UNKW") || fields[2].toString().trim().toUpperCase().startsWith("ARR") || fields[2].toString().trim().toUpperCase().startsWith("R25")) {
            return false;
        }
        if (fields[3].toString().trim().toUpperCase().startsWith("UNKW") || fields[3].toString().trim().toUpperCase().startsWith("ARR")) {
            return false;
        }
        if (fields[4].toString().trim().toUpperCase().startsWith("UNKW") || fields[4].toString().trim().toUpperCase().startsWith("ARR")) {
            return false;
        }
        return true;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance((Configuration)conf, (String)"duplicate data elimination");
        job.setJarByClass((Class)q1.class);
        job.setMapperClass((Class)Stage1Mapper.class);
        job.setReducerClass((Class)Stage1Reducer.class);
        job.setOutputKeyClass((Class)Text.class);
        job.setOutputValueClass((Class)IntWritable.class);
        FileInputFormat.addInputPath((Job)job, (Path)new Path(args[1]));
        FileOutputFormat.setOutputPath((Job)job, (Path)new Path(args[2]));
        job.waitForCompletion(true);
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance((Configuration)conf2, (String)"days data parsing");
        job2.setJarByClass((Class)q1.class);
        job2.setMapperClass((Class)Stage2Mapper.class);
        job2.setReducerClass((Class)Stage2Reducer.class);
        job2.setOutputKeyClass((Class)Text.class);
        job2.setOutputValueClass((Class)IntWritable.class);
        FileInputFormat.addInputPath((Job)job2, (Path)new Path(args[2]));
        FileOutputFormat.setOutputPath((Job)job2, (Path)new Path(args[3]));
        job2.waitForCompletion(true);
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance((Configuration)conf3, (String)"semester_day classroom usage");
        job3.setJarByClass((Class)q1.class);
        job3.setMapperClass((Class)Stage3Mapper.class);
        job3.setReducerClass((Class)Stage3Reducer.class);
        job3.setOutputKeyClass((Class)Text.class);
        job3.setOutputValueClass((Class)Text.class);
        FileInputFormat.addInputPath((Job)job3, (Path)new Path(args[3]));
        FileOutputFormat.setOutputPath((Job)job3, (Path)new Path(args[4]));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }

    public static class Stage1Mapper
    extends Mapper<Object, Text, Text, IntWritable> {
        private IntWritable intValue;
        private Text word = new Text();

        public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable> context) throws IOException, InterruptedException {
            try {
                String input = value.toString();
                boolean flag = q1.cleanData(input);
                if (!flag) {
                    return;
                }
                String[] fields = input.split(",");
                String semName = fields[1];
                String location = fields[2];
                String days = fields[3];
                String timeSlot = fields[4];
                String courseName = fields[6];
                String[] loc = location.toString().split(" ");
                if (loc.length == 1) {
                    return;
                }
                if (loc[1].trim().toUpperCase().equals("ARR") || loc[1].trim() == "") {
                    return;
                }
                StringBuilder sb = new StringBuilder();
                sb.append(semName).append("~").append(location).append("~").append(days).append("~").append(timeSlot).append("~").append(courseName);
                this.word.set(sb.toString());
                this.intValue = new IntWritable(1);
                context.write((Object)this.word, (Object)this.intValue);
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
        private IntWritable reducerValue;
        private Text reducerKey;

        public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable> context2) throws IOException, InterruptedException {
            this.reducerKey = key;
            this.reducerValue = new IntWritable(1);
            context2.write((Object)this.reducerKey, (Object)this.reducerValue);
        }
    }

    public static class Stage2Mapper
    extends Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();
        private IntWritable intValue;

        public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable> context) throws IOException, InterruptedException {
            try {
                String[] fields = value.toString().split("\\t");
                String[] ipkey = fields[0].toString().split("~");
                String semName = ipkey[0];
                String location = ipkey[1];
                String days = ipkey[2];
                char[] daysChar = days.toCharArray();
                if (days.contains("-")) {
                    ArrayList<String> allDays = new ArrayList<String>();
                    allDays.add("M");
                    allDays.add("T");
                    allDays.add("W");
                    allDays.add("R");
                    allDays.add("F");
                    allDays.add("S");
                    allDays.add("U");
                    int startIndex = allDays.indexOf(String.valueOf(daysChar[0]));
                    int endIndex = allDays.indexOf(String.valueOf(daysChar[2]));
                    int i = startIndex;
                    while (i <= endIndex) {
                        String day = (String)allDays.get(i);
                        StringBuilder sb2 = new StringBuilder();
                        sb2.append(semName).append("~").append(day).append("~").append(location);
                        this.word.set(sb2.toString());
                        this.intValue = new IntWritable(1);
                        context.write((Object)this.word, (Object)this.intValue);
                        ++i;
                    }
                } else {
                    char[] i = daysChar;
                    int endIndex = i.length;
                    int startIndex = 0;
                    while (startIndex < endIndex) {
                        char d = i[startIndex];
                        StringBuilder sb2 = new StringBuilder();
                        sb2.append(semName).append("~").append(d).append("~").append(location);
                        this.word.set(sb2.toString());
                        this.intValue = new IntWritable(1);
                        context.write((Object)this.word, (Object)this.intValue);
                        ++startIndex;
                    }
                }
            }
            catch (NumberFormatException fields) {
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class Stage2Reducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable reducerValue;
        private Text reducerKey;

        public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable> context2) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            this.reducerKey = key;
            this.reducerValue = new IntWritable(sum);
            context2.write((Object)this.reducerKey, (Object)this.reducerValue);
        }
    }

    public static class Stage3Mapper
    extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Mapper<Object, Text, Text, Text> context) throws IOException, InterruptedException {
            try {
                String[] fields = value.toString().split("\\t");
                String[] ipkey = fields[0].split("~");
                StringBuilder sb = new StringBuilder();
                int count = Integer.parseInt(fields[1]);
                sb.append(ipkey[0]).append("~").append(ipkey[1]).append("~").append(String.format("%04d", count));
                Text map2Key = new Text(sb.toString());
                Text map2Val = new Text(ipkey[2].toString());
                context.write((Object)map2Key, (Object)map2Val);
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
        private String minRoom;
        private String maxRoom;
        private String prevKey = "";
        int utilMin = 0;
        int utilMax = 0;
        int utilAvg = 0;
        int utilSum = 0;
        int utilCount = 0;

        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text> context) throws IOException, InterruptedException {
            String strKey = key.toString();
            String[] KeyArr = strKey.split("~");
            String SemDay = String.valueOf(KeyArr[0]) + "~" + KeyArr[1];
            String utilization = KeyArr[2];
            Iterator<Text> valIter = values.iterator();
            if (valIter.hasNext()) {
                String roomNum = valIter.next().toString();
                if (SemDay.equalsIgnoreCase(this.prevKey)) {
                    this.utilSum += Integer.parseInt(utilization);
                    ++this.utilCount;
                    this.utilMax = Integer.parseInt(utilization);
                    this.maxRoom = roomNum;
                } else {
                    int val;
                    if (!this.prevKey.isEmpty()) {
                        if (this.utilCount == 0) {
                            ++this.utilCount;
                        }
                        this.utilAvg = this.utilSum / this.utilCount;
                        this.reducerKey.set(this.prevKey);
                        StringBuilder sb = new StringBuilder();
                        sb.append(String.format("%04d", this.utilMin)).append("~").append(this.minRoom).append("~");
                        sb.append(String.format("%04d", this.utilMax)).append("~").append(this.maxRoom).append("~");
                        sb.append(String.format("%04d", this.utilAvg));
                        this.reducerValue.set(sb.toString());
                        context.write((Object)this.reducerKey, (Object)this.reducerValue);
                    }
                    this.prevKey = SemDay.toString();
                    this.utilMin = val = Integer.parseInt(utilization);
                    this.utilMax = val;
                    this.minRoom = roomNum;
                    this.maxRoom = roomNum;
                    this.utilSum = val;
                    this.utilCount = 1;
                }
                return;
            }
        }

        public void cleanup(Reducer<Text, Text, Text, Text> context) throws IOException, InterruptedException {
            if (this.utilCount == 0) {
                ++this.utilCount;
            }
            this.utilAvg = this.utilSum / this.utilCount;
            this.reducerKey.set(this.prevKey);
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%04d", this.utilMin)).append("~").append(this.minRoom).append("~");
            sb.append(String.format("%04d", this.utilMax)).append("~").append(this.maxRoom).append("~");
            sb.append(String.format("%04d", this.utilAvg));
            this.reducerValue.set(sb.toString());
            context.write((Object)this.reducerKey, (Object)this.reducerValue);
        }
    }

}

