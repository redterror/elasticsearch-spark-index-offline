package com.parmarh.elasticsearch;

import com.google.common.base.Supplier;
import com.google.common.io.Files;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.parmarh.elasticsearch.util.ElasticSearchPartitioner;
import com.parmarh.elasticsearch.util.EsUtils;
import com.parmarh.elasticsearch.util.S3Client;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ESIndexSnapshotJob implements Serializable {

    public static final char TUPLE_SEPARATOR = '|';
    private static final String MY_BACKUP_REPO = "my_backup_repo";
    private static final int TIMEOUT = 10000;
    private static transient Logger logger = LoggerFactory.getLogger(ESIndexSnapshotJob.class);
    private static Random random = new Random();
    protected transient JavaSparkContext sc;
    protected File tempDir;
    List<String> routes = new ArrayList<>();
    int numOfPartitions = 0;
    private String snapshotRepoName;

    public static void main(String[] args) throws Exception {

        String inputLocation = args[0];

        String snapshotFinalDestination = args[1];

        int numOfPartitions = Integer.parseInt(args[2]);

        String templateJsonPath = args[3];


        // indexName and indexType has to be consistent with mapping
        // Template JSON
        String indexName = args[4];

        String indexType = args[5];

        String snapWorkingBase = "/tmp/bulkload1/";

        String esWorkingBaseDir = "/tmp/esrawdata1/";

        ESIndexSnapshotJob job = new ESIndexSnapshotJob();

        job.setUp(numOfPartitions);

        job.createSnapshot(snapWorkingBase,
                esWorkingBaseDir,
                snapshotFinalDestination,
                inputLocation,
                indexName,
                indexType,
                templateJsonPath);

    }

    public void setUp(int numOfPartitions) throws Exception {
        this.numOfPartitions = numOfPartitions;
        tempDir = Files.createTempDir();
        tempDir.deleteOnExit();
        SparkConf sparkConf = new SparkConf();
        //sparkConf.setMaster("local[*]");
        //sparkConf.setMaster("spark://10.0.3.15:7077");
        sparkConf.setAppName(getClass().getSimpleName());
        sparkConf.set("spark.local.dir", tempDir + "/spark");
        sc = new JavaSparkContext(sparkConf);
        sc.setCheckpointDir(tempDir + "/checkpoint/");
        routes = EsUtils.getRoutes(numOfPartitions);
    }

    private void createSnapshot(String snapshotHDFSBase,
                                String esWorkingBaseDir,
                                String finalS3SnapshotDest,
                                String inputLocation,
                                String indexName,
                                String indexType,
                                String templateJsonPath)
            throws MalformedURLException, URISyntaxException, IOException, Exception {

        final int numOfPartitions = this.numOfPartitions;

        String templateJson = S3Client.readFile(templateJsonPath);

        Map<String, String> additionalEsSettings = new HashMap<String, String> ();

        ESFilesTransport transport = new ESFilesTransport();

        snapshotRepoName = MY_BACKUP_REPO;

        final ESIndexShardSnapshotCreator creator = new ESIndexShardSnapshotCreator
                (transport,
                        additionalEsSettings,
                        snapshotHDFSBase,
                        finalS3SnapshotDest,
                        snapshotRepoName,
                        esWorkingBaseDir,
                        indexType, //should be consistent with mapping json file
                        templateJson,
                        numOfPartitions,
                        100,
                        1024);


        Supplier<Configuration> configurationSupplier = new ConfigSupplier();
        final int bulkSize = 10000;
        ESIndexShardSnapshotPipeline<String, String> pipeline = new ESIndexShardSnapshotPipeline<>(
                creator,
                configurationSupplier,
                indexName,
                indexType,
                bulkSize,
                TIMEOUT);

        long start = System.currentTimeMillis();

        JavaRDD<String> jsonRDD = sc.textFile(inputLocation);


        JavaPairRDD<String, String> pairRDD = jsonRDD.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                JsonObject jsonObject = new JsonParser().parse(s.toString()).getAsJsonObject();
                // if you have unique value in your json, you can use it as key
                // instead of random num
                String key = routes.get(random.nextInt(numOfPartitions));
                String value = jsonObject.toString();

                return new Tuple2<>(key, value);
            }
        });


        JavaPairRDD<String, String> partPairRDD =
                pairRDD.partitionBy(new ElasticSearchPartitioner(numOfPartitions));

        pipeline.process(partPairRDD);

        FileUtils.deleteQuietly(tempDir);

        System.out.println("####################################### DONE ##########################################");

        System.out.println("Everything took: " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start)
                + " secs");
    }

    static final class ConfigSupplier implements Supplier<Configuration>, Serializable {
        @Override
        public Configuration get() {
            return new Configuration();
        }
    }

}
