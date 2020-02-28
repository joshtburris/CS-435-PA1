import java.io.*;
import java.nio.file.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import sun.reflect.generics.tree.Tree;
import org.apache.log4j.Logger;

public class PA1 {

    // Profile 1
    public static class Profile1Mapper extends Mapper<Object, Text, Text, IntWritable> {

        private Text word = new Text();
        private TreeMap<Text, Integer> treeMap = new TreeMap<>();

        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {

            if (value == null || value.toString().isEmpty())
                return;

            StringTokenizer itr = new StringTokenizer(
                    value.toString().split("<====>")[2]);

            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                StringBuilder builder = new StringBuilder(token.length());

                for (char c : token.toCharArray()) {
                    if ((c >= 48 && c <= 57) || (c >= 65 && c <= 90)
                            || (c >= 97 && c <= 122)) {
                        builder.append(Character.toLowerCase(c));
                    }
                }

                if (builder.toString().isEmpty())
                    return;

                word = new Text(builder.toString());

                Integer v = treeMap.get(word);
                if (v != null) {
                    treeMap.replace(word, v + 1);
                } else {
                    treeMap.put(word, 1);
                    if (treeMap.size() > 500) {
                        treeMap.pollLastEntry();
                    }
                }

            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Text, Integer> entry : treeMap.entrySet()) {
                context.write(entry.getKey(), new IntWritable(entry.getValue()));
            }
        }

    }

    public static class Profile1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Text word = new Text();
        private TreeMap<Text, Integer> treeMap = new TreeMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            Integer sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            word = new Text(key.toString());

            treeMap.put(word, sum);
            if (treeMap.size() > 500) {
                treeMap.pollLastEntry();
            }

        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Text, Integer> entry : treeMap.entrySet()) {
                context.write(entry.getKey(), new IntWritable(entry.getValue()));
            }
        }

    }

    // Profile 2
    public static class Profile2Mapper1 extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();
        private IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {

            if (value == null || value.toString().isEmpty())
                return;

            String[] data = value.toString().split("<====>");
            String docID = data[1];
            StringTokenizer itr = new StringTokenizer(data[2]);

            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                StringBuilder builder = new StringBuilder(token.length());

                for (char c : token.toCharArray()) {
                    if ((c >= 48 && c <= 57) || (c >= 65 && c <= 90)
                            || (c >= 97 && c <= 122)) {
                        builder.append(Character.toLowerCase(c));
                    }
                }

                if (builder.toString().isEmpty())
                    return;

                word.set(docID + "\t" + builder.toString());
                context.write(word, new Text("1"));
            }
        }

    }

    public static class Profile2Reducer1 extends Reducer<Text, Text, Text, Text> {

        private Text word = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Integer sum = 0;

            for (Text val : values) {
                sum += Integer.parseInt(val.toString());
            }

            String[] data = key.toString().split("\t");

            word = new Text(data[0]);

            context.write(word, new Text(data[1] + "\t" + sum));
        }

    }

    public static class Profile2Mapper2 extends Mapper<Object, Text, Text, NullWritable> {

        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {
            word.set(value.toString());
            context.write(word, NullWritable.get());
        }

    }

    public static class Profile2Reducer2 extends Reducer<Text, NullWritable, Text, NullWritable> {

        private Text word = new Text();
        private static TreeMap<Text, Integer> treeMap = new TreeMap<>(
                new Profile2SortComparator());
        private static TreeMap<Integer, Integer> docIdMap = new TreeMap<>();

        public void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {

            // Store 500 unigrams per document
            Integer docId = Integer.parseInt(key.toString().split("\t")[0]);
            Integer v = docIdMap.get(docId);
            if (v != null) {
                if (v < 500) {
                    docIdMap.replace(docId, v + 1);
                    word = new Text(key.toString());
                    treeMap.put(word, 1);
                }
            } else {
                docIdMap.put(docId, 1);
                word = new Text(key.toString());
                treeMap.put(word, 1);
            }

        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Text, Integer> entry : treeMap.entrySet()) {
                context.write(entry.getKey(), NullWritable.get());
            }
        }

    }

    public static class Profile2SortComparator extends WritableComparator {

        protected Profile2SortComparator() {
            super(Text.class, true);
        }

        public int compare(WritableComparable w0, WritableComparable w1) {

            Text t0 = (Text) w0;
            Text t1 = (Text) w1;

            String[] items0 = t0.toString().split("\t");
            String[] items1 = t1.toString().split("\t");

            Integer docId0 = Integer.parseInt(items0[0]);
            Integer docId1 = Integer.parseInt(items1[0]);
            String unigram0 = items0[1];
            String unigram1 = items1[1];
            Integer freq0 = Integer.parseInt(items0[2]);
            Integer freq1 = Integer.parseInt(items1[2]);

            Integer comparison = docId0.compareTo(docId1);
            if (comparison == 0) {
                comparison = -1 * freq0.compareTo(freq1);
            }
            if (comparison == 0) {
                comparison = unigram0.compareTo(unigram1);
            }

            return comparison;
        }

    }

    public static class UnigramPartitioner1 extends Partitioner<Text, Text> {
        private Logger LOGGER = Logger.getLogger(UnigramPartitioner1.class.getName());
        public int getPartition(Text key, Text value, int numReduceTasks) {
            Integer output = Integer.parseInt(key.toString().split("\t")[0]) % numReduceTasks;
            LOGGER.info(key.toString() + "\t" + output);
            return output;
        }
    }

    public static class UnigramPartitioner2 extends Partitioner<Text, NullWritable> {
        private Logger LOGGER = Logger.getLogger(UnigramPartitioner2.class.getName());
        public int getPartition(Text key, NullWritable value, int numReduceTasks) {
            Integer output = Integer.parseInt(key.toString().split("\t")[0]) % numReduceTasks;
            LOGGER.info(key.toString() + "\t" + output);
            return output;
        }
    }

    // Profile 3
    public static class Profile3Mapper extends Mapper<Object, Text, Text, IntWritable> {

        private Text word = new Text();
        private IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {

            if (value == null || value.toString().isEmpty())
                return;

            StringTokenizer itr = new StringTokenizer(
                    value.toString().split("<====>")[2]);

            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                StringBuilder builder = new StringBuilder(token.length());

                for (char c : token.toCharArray()) {
                    if ((c >= 48 && c <= 57) || (c >= 65 && c <= 90)
                            || (c >= 97 && c <= 122)) {
                        builder.append(Character.toLowerCase(c));
                    }
                }

                if (builder.toString().isEmpty())
                    return;

                word.set(builder.toString());

                context.write(word, one);
            }
        }
    }

    public static class Profile3Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Text word = new Text();
        private static HashMap<Text, Integer> hashMap = new HashMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            Integer sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            word = new Text(key.toString());

            Integer v = hashMap.get(word);
            if (v != null) {
                hashMap.replace(word, v + sum);
                hashMap.put(word, sum);
            } else {
                hashMap.put(word, sum);
            }
            hashMap.put(key, sum);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<Text, Integer>> list = new LinkedList<Map.Entry<Text, Integer>>(
                    hashMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<Text, Integer>>() {
                @Override
                public int compare(Map.Entry<Text, Integer> o1, Map.Entry<Text, Integer> o2) {
                    Integer comp = -o1.getValue().compareTo(o2.getValue());
                    if (comp == 0)
                        comp = -o1.getKey().toString().compareTo(o2.getKey().toString());
                    return comp;
                }
            });

            ListIterator<Map.Entry<Text, Integer>> itr = list.listIterator();
            for (int i = 0; i < 500; ++i) {
                Map.Entry<Text, Integer> entry = itr.next();
                context.write(entry.getKey(), new IntWritable(entry.getValue()));
            }
        }

    }

    // Main
    public static void main(String[] args) {

        if (args[0].compareTo("profile1") == 0) {
            System.out.println("***************** Profile 1 *****************");
            profile1(args[1], args[2]);
        } else if (args[0].compareTo("profile2") == 0) {
            System.out.println("***************** Profile 2 *****************");
            profile2(args[1], args[2]);
        } else if (args[0].compareTo("profile3") == 0) {
            System.out.println("***************** Profile 3 *****************");
            profile3(args[1], args[2]);
        }

    }

    private static void profile1(String input, String output) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Profile 1");
            job.setJarByClass(PA1.class);
            job.setMapperClass(Profile1Mapper.class);
            job.setCombinerClass(Profile1Reducer.class);
            job.setReducerClass(Profile1Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setNumReduceTasks(1);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void profile2(String input, String output) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Profile 2 part 1");
            job.setNumReduceTasks(10);
            job.setJarByClass(PA1.class);
            job.setMapperClass(Profile2Mapper1.class);
            job.setReducerClass(Profile2Reducer1.class);
            job.setPartitionerClass(UnigramPartitioner1.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path("job1Output"));
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Profile 2 part 2");
            job.setNumReduceTasks(10);
            job.setJarByClass(PA1.class);
            job.setMapperClass(Profile2Mapper2.class);
            job.setReducerClass(Profile2Reducer2.class);
            job.setPartitionerClass(UnigramPartitioner2.class);
            job.setSortComparatorClass(Profile2SortComparator.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            FileInputFormat.addInputPath(job, new Path("job1Output"));
            FileOutputFormat.setOutputPath(job, new Path(output));
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void profile3(String input, String output) {

        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Profile 3");
            job.setJarByClass(PA1.class);
            job.setNumReduceTasks(1);
            job.setMapperClass(Profile3Mapper.class);
            job.setReducerClass(Profile3Reducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}