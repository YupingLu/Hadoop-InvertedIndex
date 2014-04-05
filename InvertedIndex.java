package org.myorg;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class InvertedIndex extends Configured implements Tool {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    static enum Counters { INPUT_WORDS }
    private final static Text summary = new Text();

    private Text filename;
    private Text word = new Text();

    private boolean caseSensitive = true;
    private Set<String> patternsToSkip = new HashSet<String>();

    private long numRecords = 0;
    private String inputFile;

    public void configure(JobConf job) {
      caseSensitive = job.getBoolean("invertedindex.case.sensitive", true);
      inputFile = job.get("map.input.file");
     
      filename = new Text(new File(inputFile).getName());

      if (job.getBoolean("invertedindex.skip.patterns", false)) {
        Path[] patternsFiles = new Path[0];
        try {
          patternsFiles = DistributedCache.getLocalCacheFiles(job);
        } catch (IOException ioe) {
          System.err.println("Caught exception while getting cached files: " + org.apache.hadoop.util.StringUtils.stringifyException(ioe));
        }
        for (Path patternsFile : patternsFiles) {
          parseSkipFile(patternsFile);
        }
      }
    }

    private void parseSkipFile(Path patternsFile) {
      try {
        BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
        String pattern = null;
        while ((pattern = fis.readLine()) != null) {
          patternsToSkip.add(pattern);
        }
      } catch (IOException ioe) {
        System.err.println("Caught exception while parsing the cached file '" + patternsFile + "' : " + org.apache.hadoop.util.StringUtils.stringifyException(ioe));
      }
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      
      String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
      String[] lines = new String[2];
      lines = line.split("::", 2);
      
      line = lines[1];
      
      for (String pattern : patternsToSkip) {
        line = line.replaceAll(pattern, " ");
      }
      
      summary.set(filename.toString() + "|" + lines[0].replaceAll(" ", ""));
      
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        output.collect(word, summary);
        reporter.incrCounter(Counters.INPUT_WORDS, 1);
      }

      if ((++numRecords % 100) == 0) {
        reporter.setStatus("Finished processing " + numRecords + " records " + "from the input file: " + inputFile);
      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    private Text filenames = new Text();
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      HashSet<Text> filenamelist = new HashSet<Text>();
      
      while (values.hasNext()) {
        filenamelist.add(new Text(values.next()));
      }

      filenames.set(new Text(StringUtils.join(filenamelist, ",")));
      output.collect(key, filenames);
    }
  }

  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), InvertedIndex.class);
    conf.setJobName("invertedindex");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    List<String> other_args = new ArrayList<String>();
    for (int i=0; i < args.length; ++i) {
      if ("-skip".equals(args[i])) {
        DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf);
        conf.setBoolean("invertedindex.skip.patterns", true);
      } else {
        other_args.add(args[i]);
      }
    }

    FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
    FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

    JobClient.runJob(conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new InvertedIndex(), args);
    System.exit(res);
  }
}