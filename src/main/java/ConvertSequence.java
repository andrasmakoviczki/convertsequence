import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by AMakoviczki on 2018. 04. 30..
 */
public class ConvertSequence {
    private static final Logger LOG = LoggerFactory.getLogger(ConvertSequence.class);

    public static void main(String[] args) throws IOException {

        if (args.length < 1) {
            System.out.println("Usage: convertsequence "
                    + " <hdfs_path>");
            System.exit(1);
        }

        String hdfsPath = "hdfs://" + args[0];

        Configuration conf = new Configuration();
        conf.set("fs.default.name", hdfsPath);

        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path(hdfsPath);

        if (!fs.exists(inputPath)) {
            System.out.println("Path " + inputPath + " does not exists");
            System.exit(1);
        }

        FileStatus[] fileStatuses = fs.listStatus(inputPath);
        for (FileStatus status : fileStatuses
                ) {
            System.out.println(status.getPath().toString());
        }


    }
}
