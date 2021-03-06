package hu.elte.bigdata.thesis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * Created by AMakoviczki on 2018. 04. 30..
 */
public class ConvertHadoopFormat {
    private static final Logger LOG = LoggerFactory.getLogger(ConvertHadoopFormat.class);

    public static void main(String[] args) throws IOException {

        if (args.length < 3) {
            System.out.println("Usage: ConvertHadoopFormat "
                    + " <input path>" + " <output path>" + " <mode>");
            System.exit(1);
        }

        //Set arguments
        String inputarg = args[0];
        String outputarg = args[1];
        String mode = args[2];

        //Set configuration
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", inputarg);
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("yarn.app.mapreduce.am.staging-dir", "/tmp");

        //Create target directory
        FileSystem fs = FileSystem.get(URI.create(inputarg), conf);
        Path inputPath = new Path(inputarg);
        Path outputPath = new Path(outputarg);

        if (!fs.exists(inputPath)) {
            System.out.println("Path " + inputPath + " does not exists");
            System.exit(1);
        }

        if (mode.equals("hbase")) {
            SequenceFile.Writer writer = null;
            writeToHbase(conf, fs, inputPath, outputarg);
        } else if (mode.equals("seqnc")) {
            try {
                SequenceFile.Writer writer = null;
                write(conf, fs, inputPath, outputPath, writer, null, null);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (mode.equals("seqrc")) {
            SequenceFile.Writer writer = null;
            try {
                write(conf, fs, inputPath, outputPath, writer, SequenceFile.CompressionType.RECORD, new BZip2Codec());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (mode.equals("seqbc")) {
            SequenceFile.Writer writer = null;
            try {
                write(conf, fs, inputPath, outputPath, writer, SequenceFile.CompressionType.BLOCK, new BZip2Codec());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (mode.equals("map")) {
            MapFile.Writer writer = null;
            try {
                writeMap(conf, fs, inputPath, outputPath, writer, SequenceFile.CompressionType.BLOCK, new BZip2Codec());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (mode.equals("readseq")) {
            SequenceFile.Reader reader = null;
            try {
                readSeq(conf, fs, outputPath, reader);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (mode.equals("readmap")) {
            MapFile.Reader reader = null;
            try {
                readMap(conf, fs, outputPath, reader);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void write(Configuration conf, FileSystem fs, Path inputPath, Path outputPath, SequenceFile.Writer writer, SequenceFile.CompressionType cType, CompressionCodec cCodec) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(inputPath);

        //Initialize writer
        writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(outputPath),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(BytesWritable.class));

        //Write input into SequenceFile
        for (FileStatus status : fileStatuses) {
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

        //Initialize writer
        if (!cType.equals(null)) {
            writer = new MapFile.Writer(conf,
                    outputPath,
                    MapFile.Writer.keyClass(Text.class),
                    MapFile.Writer.valueClass(BytesWritable.class),
                    SequenceFile.Writer.valueClass(BytesWritable.class),
                    SequenceFile.Writer.compression(cType, cCodec));
        } else {
            writer = new MapFile.Writer(conf,
                    outputPath,
                    MapFile.Writer.keyClass(Text.class),
                    MapFile.Writer.valueClass(BytesWritable.class),
                    SequenceFile.Writer.valueClass(BytesWritable.class));
        }


        //Write input into MapFile
        for (FileStatus status : fileStatuses) {
            FSDataInputStream file = fs.open(status.getPath());
            byte buffer[] = new byte[file.available()];
            file.read(buffer);
            writer.append(new Text(status.getPath().toString()), new BytesWritable(buffer));
            file.close();
        }
        IOUtils.closeStream(writer);
    }

    public static void writeToHbase(Configuration conf, FileSystem fs, Path inputPath, String tname) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(inputPath);

        //HBase configuration
        Configuration hconf = HBaseConfiguration.create();
        hconf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
        hconf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));

        //Set connecton
        Connection connection = ConnectionFactory.createConnection(hconf);
        Admin admin = connection.getAdmin();

        //Create HBase table if not exists
        TableName tableName = TableName.valueOf(tname);
        if (!admin.tableExists(tableName)) {
            HTableDescriptor hbaseTable = new HTableDescriptor(tableName);
            HColumnDescriptor hbaseColumn = new HColumnDescriptor("image");
            hbaseColumn.setMobEnabled(true);
            hbaseColumn.setMobThreshold(102400L);
            hbaseTable.addFamily(hbaseColumn);
            admin.createTable(hbaseTable);

        }

        Table hTable = connection.getTable(tableName);

        //Insert data into table
        for (FileStatus status : fileStatuses) {
            FSDataInputStream file = fs.open(status.getPath());
            byte buffer[] = new byte[file.available()];
            file.read(buffer);

            Put p = new Put(Bytes.toBytes(status.getPath().toString()));
            p.addColumn(Bytes.toBytes("image"), Bytes.toBytes("img"), buffer);
            hTable.put(p);

            file.close();
        }
    }

    public static void readSeq(Configuration conf, FileSystem fs, Path outputPath, SequenceFile.Reader reader) throws IOException {
        //Open file
        reader = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(outputPath));

        Text key = new Text();
        BytesWritable value = new BytesWritable();

        //Read content
        while (reader.next(key, value)) {
            System.out.println(key.toString() + " " + value.getLength());
        }
        IOUtils.closeStream(reader);
    }

    public static void readMap(Configuration conf, FileSystem fs, Path outputPath, MapFile.Reader reader) throws IOException {
        //Open file
        reader = new MapFile.Reader(outputPath, conf);

        Text key = new Text();
        BytesWritable value = new BytesWritable();

        //Read content
        while (reader.next(key, value)) {
            System.out.println(key.toString() + " " + value.getLength());
        }
        IOUtils.closeStream(reader);
    }

}
