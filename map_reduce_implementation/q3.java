import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class q3 {
    public static boolean cleanData(String str) {
        String[] fields = str.split(",");
        if (fields.length > 9) {
            return false;
        }
        if (fields[2].toString().trim().toUpperCase().startsWith("UNK") || fields[2].toString().trim().toUpperCase().startsWith("ARR") || fields[2].toString().trim().toUpperCase().startsWith("R25")) {
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
        Job job = Job.getInstance((Configuration)conf, (String)"class utilization");
        job.setJarByClass((Class)q3.class);
        job.setMapperClass((Class)TokenizerMapper.class);
        job.setReducerClass((Class)classUtlization.class);
        job.setOutputKeyClass((Class)Text.class);
        job.setOutputValueClass((Class)Text.class);
        FileInputFormat.addInputPath((Job)job, (Path)new Path(args[1]));
        FileOutputFormat.setOutputPath((Job)job, (Path)new Path(args[2]));
        job.waitForCompletion(true);
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance((Configuration)conf, (String)"Hall utilization");
        job2.setJarByClass((Class)q3.class);
        job2.setMapperClass((Class)Stage2Mapper.class);
        job2.setReducerClass((Class)Stage2Reducer.class);
        job2.setOutputKeyClass((Class)Text.class);
        job2.setOutputValueClass((Class)Text.class);
        FileInputFormat.addInputPath((Job)job2, (Path)new Path(args[2]));
        FileOutputFormat.setOutputPath((Job)job2, (Path)new Path(args[3]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

    public static class Stage2Mapper
    extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Mapper<Object, Text, Text, Text> context) throws IOException, InterruptedException {
            try {
                String[] fields = value.toString().split("\\t");
                Text map2Key = new Text(fields[0].toString());
                Text map2Val = new Text(fields[1].toString());
                context.write((Object)map2Key, (Object)map2Val);
                System.out.println("Mapper2 output: " + fields[0].toString() + "/" + fields[1].toString());
            }
            catch (NumberFormatException e) {
                System.out.println("NumberFormatException occured... ");
                System.out.println("Input causing the error: " + value.toString());
            }
            catch (Exception e) {
                System.out.println("Some other exception ... refer logs");
                System.out.println("Input causing the error: " + value.toString());
                e.printStackTrace();
            }
        }
    }

    public static class Stage2Reducer
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
            String[] KeyArr = strKey.split("_");
            String hall = KeyArr[0];
            String utilization = KeyArr[1];
            System.out.println("prev Hall: " + this.prevKey);
            Iterator<Text> valIter = values.iterator();
            String roomNum = valIter.next().toString();
            System.out.println("key/value:Sum/Max/Min/Count: " + strKey + "/" + roomNum + ":" + this.utilSum + "/" + this.utilMax + "/" + this.utilMin + "/" + this.utilCount);
            if (hall.equalsIgnoreCase(this.prevKey)) {
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
                    sb.append(String.format("%04d", this.utilMin)).append("_").append(this.minRoom).append("_");
                    sb.append(String.format("%04d", this.utilMax)).append("_").append(this.maxRoom).append("_");
                    sb.append(String.format("%04d", this.utilAvg));
                    this.reducerValue.set(sb.toString());
                    context.write((Object)this.reducerKey, (Object)this.reducerValue);
                }
                this.prevKey = hall.toString();
                this.utilMin = val = Integer.parseInt(utilization);
                this.minRoom = roomNum;
                this.utilSum = val;
                this.utilCount = 1;
            }
        }

        public void cleanup(Reducer<Text, Text, Text, Text> context) throws IOException, InterruptedException {
            if (this.utilCount == 0) {
                ++this.utilCount;
            }
            this.utilAvg = this.utilSum / this.utilCount;
            this.reducerKey.set(this.prevKey);
            if (this.utilMax == 0) {
                this.utilMax = this.utilMin;
                this.maxRoom = this.minRoom;
            }
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%04d", this.utilMin)).append("_").append(this.minRoom).append("_");
            sb.append(String.format("%04d", this.utilMax)).append("_").append(this.maxRoom).append("_");
            sb.append(String.format("%04d", this.utilAvg));
            this.reducerValue.set(sb.toString());
            context.write((Object)this.reducerKey, (Object)this.reducerValue);
        }
    }

    public static class TokenizerMapper
    extends Mapper<Object, Text, Text, Text> {
        private Text intValue = new Text();
        private Text word = new Text();

        public void map(Object key, Text value, Mapper<Object, Text, Text, Text> context) throws IOException, InterruptedException {
            try {
                String input = value.toString();
                boolean flag = q3.cleanData(input);
                if (!flag) {
                    return;
                }
                String[] fields = input.split(",");
                String semId = fields[0];
                String semName = fields[1];
                String location = fields[2];
                String days = fields[3];
                String timeSlot = fields[4];
                String courseNum = fields[5];
                String courseName = fields[6];
                String[] loc = location.toString().split(" ");
                if (loc.length == 1) {
                    return;
                }
                if (loc[1].trim().toUpperCase().equals("ARR") || loc[1].trim() == "") {
                    return;
                }
                StringBuilder sb = new StringBuilder();
                sb.append(loc[0].trim()).append("_").append(loc[1].trim());
                int maxStudents = Integer.parseInt(fields[8]);
                String curStudents = String.format("%04d", Integer.parseInt(fields[7]));
                if (curStudents.equals("0000")) {
                    return;
                }
                this.intValue = new Text(curStudents);
                this.word.set(sb.toString());
                context.write((Object)this.word, (Object)this.intValue);
                System.out.println("Mapper output: " + sb.toString() + "/" + curStudents + " / day: " + days.trim());
            }
            catch (NumberFormatException e) {
                System.out.println("NumberFormatException occured... ");
                System.out.println("Input causing the error: " + value.toString());
            }
            catch (Exception e) {
                System.out.println("Some other exception ... refer logs");
                System.out.println("Input causing the error: " + value.toString());
                e.printStackTrace();
            }
        }
    }

    public static class classUtlization
    extends Reducer<Text, Text, Text, Text> {
        private Text reducerValue = new Text();
        private Text reducerKey = new Text();

        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text> context2) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            int utilization = 0;
            int max = 0;
            for (Text val : values) {
                int intVal = Integer.parseInt(val.toString());
                sum += intVal;
                ++count;
                if (intVal <= max) continue;
                max = intVal;
            }
            if (count == 0) {
                count = 1;
            }
            if (max == 0) {
                max = 1;
            }
            System.out.println("Location:Sum/Max/Count: " + key.toString() + ":" + sum + "/" + max + "/" + count);
            utilization = sum * 100 / (max * count);
            String util = String.format("%04d", utilization);
            String[] loc = key.toString().split("_");
            if (loc.length == 1) {
                return;
            }
            if (count < 10) {
                return;
            }
            String hall = loc[0];
            String room = loc[1];
            this.reducerValue.set(room);
            StringBuilder sb2 = new StringBuilder();
            sb2.append(hall).append("_").append(util);
            this.reducerKey.set(sb2.toString());
            context2.write((Object)this.reducerKey, (Object)this.reducerValue);
            System.out.println("Reducer output key/value: " + sb2.toString() + "/" + room);
        }
    }

}

