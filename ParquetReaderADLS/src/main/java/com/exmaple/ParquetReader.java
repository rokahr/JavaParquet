package com.exmaple;
import java.io.*;
import java.sql.SQLOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;



public class ParquetReader {

    public static String storageAccount="yourstoragename";
    public static String accountKey = "yourkey";
    public static String blobName="yourblobname";
    public static String folder = "yourfoldername";
    public static String fileName="filename.parquet";

    private static Path path = new Path("file:\\C:\\Users\\roman\\Downloads\\Parquet\\"+fileName);

    private static void printGroup(Group g, int r) {
        int fieldCount = g.getType().getFieldCount();
        if (r == 0) {
            for (int field = 0; field < fieldCount; field++) {
                int valueCount = g.getFieldRepetitionCount(field);
                //System.out.print(valueCount);
                Type fieldType = g.getType().getType(field);
                String fieldName = fieldType.getName();
                for (int jndex = 0; jndex < valueCount; jndex++){

                    if (fieldType.isPrimitive()) {
                        System.out.print(fieldName+"  ");
                    }
                }
            }
            System.out.println("");
        }
        for (int field = 0; field < fieldCount; field++) {
            int valueCount = g.getFieldRepetitionCount(field);
            //System.out.print(valueCount);
            Type fieldType = g.getType().getType(field);
            if(valueCount==0){
                System.out.print("null  ");
            }
            for (int index = 0; index < valueCount; index++){

                if (fieldType.isPrimitive()) {
                    System.out.print(g.getValueToString(field, index)+"  ");
                }
            }
        }
        System.out.println("");
    }

    static public DataLakeServiceClient GetDataLakeServiceClient
            (String accountName, String accountKey){

        StorageSharedKeyCredential sharedKeyCredential =
                new StorageSharedKeyCredential(accountName, accountKey);

        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();

        builder.credential(sharedKeyCredential);
        builder.endpoint("https://" + accountName + ".dfs.core.windows.net");

        return builder.buildClient();
    }

    static public void DownloadFile(DataLakeFileSystemClient fileSystemClient)
            throws FileNotFoundException, java.io.IOException{

        DataLakeDirectoryClient directoryClient =
                fileSystemClient.getDirectoryClient(folder);

        DataLakeFileClient fileClient =
                directoryClient.getFileClient(fileName);

        File file = new File("C:\\Users\\roman\\Downloads\\Parquet\\"+fileName);

        OutputStream targetStream = new FileOutputStream(file);

        fileClient.read(targetStream);

        targetStream.close();

    }

    public static void main(String[] args) throws IllegalArgumentException, IOException {



        Configuration conf = new Configuration();

        DataLakeServiceClient adlsClient = GetDataLakeServiceClient(storageAccount,accountKey);

        DataLakeFileSystemClient dataLakeFileSystemClient = adlsClient.getFileSystemClient(blobName);

        DownloadFile(dataLakeFileSystemClient);
        try {
            ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
            MessageType schema = readFooter.getFileMetaData().getSchema();
            ParquetFileReader r = new ParquetFileReader(conf, path, readFooter);

            PageReadStore pages = null;
            try {
                while (null != (pages = r.readNextRowGroup())) {
                    final long rows = pages.getRowCount();
                    //System.out.println("Number of rows: " + rows);

                    final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                    final RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                    for (int i = 0; i < rows; i++) {
                        final Group g = (Group) recordReader.read();
                        printGroup(g,i);

                        // TODO Compare to System.out.println(g);
                    }
                }
            } finally {
                r.close();
            }
        } catch (IOException e) {
            System.out.println("Error reading parquet file.");
            e.printStackTrace();
        }
        System.out.println();
        System.out.println("To load the file directly into DataBricks please copy and paste the following code snippet into your DataBricks notebook:");
        System.out.println("data = spark.read.parquet(\"abfss://"+blobName+"@"+storageAccount+".dfs.core.windows.net/"+folder+"/"+fileName+"\")");
        System.out.println("data.show()");
        System.out.println();
        System.out.println("To see all files within this folder please use copy and paste the following code snippet into your DataBricks notebook");
        System.out.println("configs = {\n" +
                "  \"fs.azure.account.auth.type\": \"CustomAccessToken\",\n" +
                "  \"fs.azure.account.custom.token.provider.class\":   spark.conf.get(\"spark.databricks.passthrough.adls.gen2.tokenProviderClassName\")\n" +
                "}");
        System.out.println("dbutils.fs.mount(\n" +
                "  source = \"abfss://"+blobName+"@"+storageAccount+".dfs.core.windows.net\",\n" +
                "  mount_point = \"/mnt/data\",\n" +
                "  extra_configs = configs)");
        System.out.println("dbutils.fs.ls(\"/mnt/data\") ");
    }
}

