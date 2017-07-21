package com.bestv.datalake.common;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Package: com.bestv.datalake.common.
 * User: SichengWang
 * Date: 2017/6/27
 * Time: 15:58
 * Project: DataLake
 */
public class JsonInputFormat extends FileInputFormat<LongWritable, CustomMapWritable> implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(JsonInputFormat.class);


    @Override
    public RecordReader<LongWritable, CustomMapWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new JsonRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec = new CompressionCodecFactory(HadoopCompat.getConfiguration(context)).getCodec(file);
        return codec == null;
    }

    public static class JsonRecordReader extends RecordReader<LongWritable, CustomMapWritable> {
        private static final Logger LOG = LoggerFactory.getLogger(JsonRecordReader.class);
        private LineRecordReader reader = new LineRecordReader();

        private final CustomMapWritable value_ = new CustomMapWritable();
        private final Gson gson = new Gson();

        @Override
        public void initialize(InputSplit split,
                               TaskAttemptContext context)
                throws IOException, InterruptedException {
            reader.initialize(split, context);
        }

        @Override
        public synchronized void close() throws IOException {
            reader.close();
        }

        @Override
        public LongWritable getCurrentKey() throws IOException,
                InterruptedException {
            return reader.getCurrentKey();
        }

        @Override
        public CustomMapWritable getCurrentValue() throws IOException,
                InterruptedException {
            return value_;
        }

        @Override
        public float getProgress()
                throws IOException, InterruptedException {
            return reader.getProgress();
        }

        @Override
        public boolean nextKeyValue()
                throws IOException, InterruptedException {
            while (reader.nextKeyValue()) {
                value_.clear();
                if (decodeLineToJson(gson, reader.getCurrentValue(), value_)) {
                    return true;
                }
            }
            return false;
        }

        public static boolean decodeLineToJson(Gson gson, Text line, CustomMapWritable value) {
            try {
                LinkedHashMap<String, String> map = gson.fromJson(new String(line.getBytes(), 0, line.getLength(), "GBK"), new TypeToken<LinkedHashMap<String, String>>() {}.getType());
                Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String, String> entry = it.next();
                    Text mapKey = new Text(entry.getKey());
                    Text mapValue = new Text(entry.getValue());
                    value.put(mapKey, mapValue);
                }
                return true;
            }  catch (NumberFormatException e) {
                LOG.warn("Could not parse field into number: " + line, e);
                return false;
            } catch (UnsupportedEncodingException e) {
                LOG.warn("Could not parse line into utf-8: " + line, e);
                return false;
            }
        }
    }

}
