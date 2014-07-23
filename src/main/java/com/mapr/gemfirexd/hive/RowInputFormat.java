package com.mapr.gemfirexd.hive;

import com.pivotal.gemfirexd.hadoop.mapred.Key;
import com.pivotal.gemfirexd.hadoop.mapred.Row;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import java.io.IOException;

public class RowInputFormat extends com.pivotal.gemfirexd.hadoop.mapred.RowInputFormat {
    public RecordReader<Key, Row> getRecordReader(org.apache.hadoop.mapred.InputSplit split, JobConf job, Reporter reporter)
            throws IOException {
        CombineFileSplit combineSplit = null;
        if (split instanceof FileSplit) {
            final FileSplit fileSplit = (FileSplit) split;
            combineSplit = new CombineFileSplit(job,
                    new Path[]{fileSplit.getPath()},
                    new long[]{fileSplit.getStart()},
                    new long[]{fileSplit.getLength()},
                    fileSplit.getLocations());
        } else {
            combineSplit = (CombineFileSplit) split;
        }

        return super.getRecordReader(combineSplit, job, reporter);
    }
}
