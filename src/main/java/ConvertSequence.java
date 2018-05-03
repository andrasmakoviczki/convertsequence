import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

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

        String hdfsPath = args[0];
        String seqPath = args[1];

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsPath);
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("yarn.app.mapreduce.am.staging-dir", "/tmp");

        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        Path inputPath = new Path(hdfsPath);
        Path outputPath = new Path(seqPath);

        if (!fs.exists(inputPath)) {
            System.out.println("Path " + inputPath + " does not exists");
            System.exit(1);
        }

        SequenceFile.Writer writer = null;

        try {
            write(conf, fs, inputPath, outputPath, writer, null,null);
        } catch (Exception e) {
            e.printStackTrace();
        }

        /*MapFile.Writer writer = null;
        try {
            writeMap(conf, fs, inputPath, outputPath, writer, SequenceFile.CompressionType.BLOCK,new GzipCodec());
        } catch (Exception e) {
            e.printStackTrace();
        }*/

        /*SequenceFile.Reader reader = null;

        try {
            read(conf,fs,outputPath,reader);
        } catch (Exception e) {
            e.printStackTrace();
        }*/

        /*MapFile.Reader reader = null;

        try {
            readMap(conf,fs,outputPath,reader);
        } catch (Exception e) {
            e.printStackTrace();
        }*/
    }

    public static void write(Configuration conf, FileSystem fs, Path inputPath, Path outputPath, SequenceFile.Writer writer, SequenceFile.CompressionType cType, CompressionCodec cCodec) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(inputPath);
        writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(outputPath),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(BytesWritable.class));

        for (FileStatus status : fileStatuses) {
            System.out.println(status.getPath().toString());

            FSDataInputStream file = fs.open(status.getPath());

            byte buffer[] = new byte[file.available()];
            file.read(buffer);

            writer.append(new Text(status.getPath().toString()), new BytesWritable(buffer));

            file.close();
        }
        IOUtils.closeStream(writer);
    }

    public static void writeMap(Configuration conf, FileSystem fs, Path inputPath, Path outputPath, MapFile.Writer writer, SequenceFile.CompressionType cType, CompressionCodec cCodec) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(inputPath);
        writer = new MapFile.Writer(conf,
                outputPath,
                MapFile.Writer.keyClass(Text.class),
                MapFile.Writer.valueClass(BytesWritable.class),
                SequenceFile.Writer.valueClass(BytesWritable.class),
                SequenceFile.Writer.compression(cType,cCodec));

        for (FileStatus status : fileStatuses) {
            System.out.println(status.getPath().toString());

            FSDataInputStream file = fs.open(status.getPath());

            byte buffer[] = new byte[file.available()];
            file.read(buffer);

            writer.append(new Text(status.getPath().toString()), new BytesWritable(buffer));

            file.close();
        }
        IOUtils.closeStream(writer);
    }

    public static void read(Configuration conf, FileSystem fs, Path outputPath, SequenceFile.Reader reader) throws IOException{
        reader = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(outputPath));

        Text key = new Text();
        BytesWritable value = new BytesWritable();

        while (reader.next(key, value)) {
            System.out.println(key.toString() + " " + value.getLength());
        }

        IOUtils.closeStream(reader);
    }

    public static void readMap(Configuration conf, FileSystem fs, Path outputPath, MapFile.Reader reader) throws IOException{
        reader = new MapFile.Reader(outputPath,conf);

        Text key = new Text();
        BytesWritable value = new BytesWritable();

        while (reader.next(key, value)) {
            System.out.println(key.toString() + " " + value.getLength());
        }

        IOUtils.closeStream(reader);
    }
}
