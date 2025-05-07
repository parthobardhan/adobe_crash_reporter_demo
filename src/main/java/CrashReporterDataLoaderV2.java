import com.mongodb.client.*;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.FindIterable;

import org.bson.Document;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

class BucketKey {
    private final String product;
    private final String version;
    private final String build;
    private final String module;
    private final int offset;

    public BucketKey(String product, String version, String build, String module, int offset) {
        this.product = product;
        this.version = version;
        this.build = build;
        this.module = module;
        this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BucketKey bucketKey = (BucketKey) o;
        return offset == bucketKey.offset &&
               Objects.equals(product, bucketKey.product) &&
               Objects.equals(version, bucketKey.version) &&
               Objects.equals(build, bucketKey.build) &&
               Objects.equals(module, bucketKey.module);
    }

    @Override
    public int hashCode() {
        return Objects.hash(product, version, build, module, offset);
    }

    @Override
    public String toString() {
        return product + "_" + version + "_" + build + "_" + module + "_" + offset;
    }
}

public class CrashReporterDataLoader {

    private static final String CONNECTION_URI =  "mongodb://localhost:27017";
    private static final String DATABASE = "CRASH";
    private static final int NUM_DOCS = 100000;
    private static final int BATCH_SIZE = 1000;

    private static final List<String> ADOBE_APPS = Arrays.asList(
            "Photoshop", "Illustrator", "Premiere Pro",
            "After Effects", "InDesign", "XD",
            "Lightroom", "Acrobat Pro", "Animate",
            "Dreamweaver"
    );

	public static void main(String[] args) {

		try (MongoClient mongoClient = MongoClients.create(CONNECTION_URI)) {
            MongoDatabase db = mongoClient.getDatabase(DATABASE);
            MongoCollection<Document> crashPkgCollection = db.getCollection("CRD_CRASH_PKG");

            // PHASE 1: Create and insert crash documents without bucket assignments
            System.out.println("PHASE 1: Creating and inserting crash documents...");
            createCrashDocuments(crashPkgCollection);

            // PHASE 2: Process crash documents to create buckets and update crash documents
            System.out.println("\nPHASE 2: Creating buckets and updating crash documents...");
            createBucketsAndUpdateCrashDocuments(mongoClient, db);

            System.out.println("\nData processing complete.");
        } catch (Exception e) {
            System.err.println("Error processing data: " + e.getMessage());
            e.printStackTrace();
        }
	}

	/**
	 * Phase 1: Create and insert crash documents without bucket assignments
	 */
	private static void createCrashDocuments(MongoCollection<Document> crashPkgCollection) {
	    Random random = new Random();
	    java.util.HashMap<String, Integer> appIdMap = new java.util.HashMap<>();
	    int nextAppId = 1000;

	    // Batch processing for better performance
	    int batchCount = 0;
	    int totalProcessed = 0;
	    ArrayList<Document> crashPkgDocsBatch = new ArrayList<>(BATCH_SIZE);

	    for (int i = 1; i <= NUM_DOCS; i++) {
	        // Randomly select an app
	        String appName = ADOBE_APPS.get(random.nextInt(ADOBE_APPS.size()));
	        int versionMajor = 2024;
	        int versionMinor = random.nextInt(10) + 1; // 1 to 10
	        String version = versionMajor + "." + versionMinor;
	        int buildNum = random.nextInt(10) + 1; // 1 to 10
	        String build = String.valueOf(buildNum);
	        String module = appName + "Core";
	        int offset = 1024 * (random.nextInt(10) + 1); // Random offset

	        // Consistent app_id for (appName, version, build) using HashMap
	        String appKey = appName + "_" + version + "_" + build;
	        Integer appId = appIdMap.get(appKey);
	        if (appId == null) {
	            appId = nextAppId++;
	            appIdMap.put(appKey, appId);
	        }

	        Document appSubDoc = new Document("APP_ID", appId)
	                .append("NAME", appName)
	                .append("VERSION", version)
	                .append("BUILD", build);

	        // CRASH PACKAGE DOCUMENT - without BUCKET_ID_ADOBE
	        Document crashPkgDoc = new Document("ID", i)
	                .append("UNIQUE_CRASH_ID", 30000 + i)
	                .append("USER_ID", 4000 + i)
	                .append("PLATFORM_ID", 2)
	                .append("MODULE", module)
	                .append("OFFSET", offset)
	                .append("EXCEPTION", getClob("Exception: Rendering failed in " + appName))
	                .append("REPROSTEPS", getClob("1. Launch " + appName + "\n2. Load project\n3. Apply effects\n4. Crash"))
	                .append("NOTES", getClob("Consistent crash in " + appName + " when using GPU acceleration."))
	                .append("EMAILDATE", Date.from(Instant.now()))
	                .append("CRASHDATE", Date.from(Instant.now()))
	                .append("CREATED", new Date())
	                .append("PACKAGE", getClob("Serialized crash report blob for " + appName))
	                .append("CHECKSUM", 987654321 + i)
	                .append("USER_EMAIL", "user" + i + "@example.com")
	                .append("CRASH_GUID", "guid-crash-" + i)
	                .append("USER_GUID", "guid-user-" + i)
	                .append("BUCKET_ID_TOP", 10 + i)
	                // BUCKET_ID_ADOBE is intentionally omitted here
	                .append("MS_EVENT_ID", 500 + i)
	                .append("MS_EVENT_TYPE", "crash")
	                .append("MS_CAB_ID", 600 + i)
	                .append("PATH", "C:/Users/user/Documents/crashes/crash" + i + ".dmp")
	                .append("APP", appSubDoc)
	                .append("DMP_PATH", "/dumps/" + appName.replaceAll(" ", "") + "_" + i + ".dmp")
	                .append("MS_EVENT_NAME", "CrashEvent-" + appName.replaceAll(" ", ""))
	                .append("OLD_CRASH_ID", null)
	                .append("OLD_BUCKET_ID", null)
	                .append("EXPLOITABLE", "NO")
	                .append("ISSUE_ID", 999999 + i)
	                .append("ISSUEMAKER_HOST", "crash-reporter.adobe.internal")
	                .append("ISSUEMAKER_TIMESTAMP", new Date())
	                .append("ISSUEMAKER_STATUS", "Open")
	                .append("CLEAN_COMMENT", getClob("Clean environment, reproducible every time."))
	                .append("COMMENT_SCORE", 8.9)
	                .append("CR_DUNAMIS_SESSIONID", "sess-" + i)
	                .append("SPLUNK_URL", "https://splunk.adobe.com/crashes/" + i)
	                .append("APPLOG_PATH", "/logs/adobe/" + i + "/app.log");

	        crashPkgDocsBatch.add(crashPkgDoc);

	        // When batch size is reached, insert the batch
	        if (crashPkgDocsBatch.size() >= BATCH_SIZE || i == NUM_DOCS) {
	            crashPkgCollection.insertMany(crashPkgDocsBatch, new InsertManyOptions().ordered(false));

	            totalProcessed += crashPkgDocsBatch.size();
	            batchCount++;

	            // Print progress
	            System.out.println("Processed batch #" + batchCount +
	                               " (" + crashPkgDocsBatch.size() + " records)" +
	                               " - Total: " + totalProcessed + "/" + NUM_DOCS);

	            // Clear the batch for next round
	            crashPkgDocsBatch.clear();
	        }
	    }
	}

	/**
	 * Phase 2: Process crash documents to create buckets and update crash documents
	 * Uses batching to handle large datasets efficiently
	 */
	private static void createBucketsAndUpdateCrashDocuments(MongoClient mongoClient, MongoDatabase db) {
	    MongoCollection<Document> bucketCollection = db.getCollection("CRD_BUCKET_ADOBE");
	    MongoCollection<Document> crashPkgCollection = db.getCollection("CRD_CRASH_PKG");

	    // Map to store bucket keys to their generated IDs
	    Map<BucketKey, Integer> bucketKeyToIdMap = new HashMap<>();
	    int nextBucketId = 10000;
	    int processedCount = 0;
	    int batchCount = 0;

	    // Process documents in batches to avoid loading all into memory
	    int batchSize = BATCH_SIZE;
	    long totalDocCount = crashPkgCollection.countDocuments();
	    System.out.println("Total crash documents to process: " + totalDocCount);

	    // Process in batches using skip and limit
	    boolean hasMoreDocs = true;
	    int skipCount = 0;

	    while (hasMoreDocs) {
	        // Get a batch of documents
	        FindIterable<Document> crashDocsBatch = crashPkgCollection.find()
	                .skip(skipCount)
	                .limit(batchSize);

	        // Track if we found any documents in this batch
	        boolean foundDocsInBatch = false;
	        ArrayList<Document> crashDocsToUpdate = new ArrayList<>();
	        ArrayList<Integer> bucketIdsForUpdate = new ArrayList<>();

	        // Process each document in the batch
	        for (Document crashDoc : crashDocsBatch) {
	            foundDocsInBatch = true;

	            // Extract the attributes needed for bucketing
	            Document appDoc = (Document) crashDoc.get("APP");
	            String product = appDoc.getString("NAME");
	            String version = appDoc.getString("VERSION");
	            String build = appDoc.getString("BUILD");
	            String module = crashDoc.getString("MODULE");
	            int offset = crashDoc.getInteger("OFFSET");

	            // Create a unique bucket key
	            BucketKey bucketKey = new BucketKey(product, version, build, module, offset);

	            // Get or create a bucket ID for this key
	            Integer bucketId = bucketKeyToIdMap.get(bucketKey);
	            if (bucketId == null) {
	                // Check if a bucket with these attributes already exists in the database
	                Document existingBucket = bucketCollection.find(new Document("APP.NAME", product)
	                        .append("APP.VERSION", version)
	                        .append("APP.BUILD", build)
	                        .append("MODULE_NAME", module)
	                        .append("OFFSET", offset)).first();

	                if (existingBucket != null) {
	                    // Use the existing bucket's ID
	                    bucketId = existingBucket.getInteger("ID");
	                } else {
	                    // Create a new bucket
	                    bucketId = nextBucketId++;

	                    Document bucketDoc = new Document("ID", bucketId)
	                            .append("MODULE_ID", 200 + bucketId)
	                            .append("APP", appDoc)
	                            .append("NAME", "Crash in " + product)
	                            .append("OFFSET", offset)
	                            .append("BUGNUM", "ADBE-CR-" + bucketId)
	                            .append("SOLUTION_URL", "https://adobe.com/solution/" + bucketId)
	                            .append("BUCKET_CREATED", new Date())
	                            .append("STATUS", 1)
	                            .append("PARENT_BUCKET", null)
	                            .append("DEVNOTES", "Issue while using " + product + " under high load.")
	                            .append("CRASH_COUNT", 1) // Start with 1 crash
	                            .append("LAST_CRASH_DATE", new Date())
	                            .append("STACK_ELEMENT", "com.adobe." + product.replaceAll(" ", "") + "::renderFunction")
	                            .append("MODULE_NAME", module)
	                            .append("USER_STEPS", "Y")
	                            .append("UNIQUE_CRASH_USER_STEPS_COUNT", 1) // Start with 1
	                            .append("PLATFORM", "Windows")
	                            .append("OS_GROUP_IDS", "WIN11,WIN10")
	                            .append("JIRA_ISSUE_IDS", "ADBEJIRA-" + (9000 + bucketId));

	                    // Insert the new bucket
	                    bucketCollection.insertOne(bucketDoc);
	                }

	                bucketKeyToIdMap.put(bucketKey, bucketId);
	            } else {
	                // Bucket exists, increment its crash count
	                bucketCollection.updateOne(
	                    new Document("ID", bucketId),
	                    new Document("$inc", new Document("CRASH_COUNT", 1)
	                            .append("UNIQUE_CRASH_USER_STEPS_COUNT", 1))
	                    .append("$set", new Document("LAST_CRASH_DATE", new Date()))
	                );
	            }

	            // Collect crash doc IDs and bucket IDs for batch update
	            crashDocsToUpdate.add(crashDoc);
	            bucketIdsForUpdate.add(bucketId);

	            processedCount++;
	        }

	        // Batch update all crash documents with their bucket IDs
	        for (int i = 0; i < crashDocsToUpdate.size(); i++) {
	            Document crashDoc = crashDocsToUpdate.get(i);
	            Integer bucketId = bucketIdsForUpdate.get(i);

	            crashPkgCollection.updateOne(
	                new Document("ID", crashDoc.getInteger("ID")),
	                new Document("$set", new Document("BUCKET_ID_ADOBE", bucketId))
	            );
	        }

	        // If we didn't find any docs in this batch, we're done
	        if (!foundDocsInBatch) {
	            hasMoreDocs = false;
	        } else {
	            // Increment skip count for next batch
	            skipCount += batchSize;
	            batchCount++;

	            // Print progress
	            System.out.println("Processed batch #" + batchCount +
	                    " - Total: " + processedCount + "/" + totalDocCount +
	                    " - Unique buckets: " + bucketKeyToIdMap.size());
	        }
	    }

	    System.out.println("Bucketing complete. Processed " + processedCount +
	            " crash documents and created " + bucketKeyToIdMap.size() + " unique buckets.");
	}

	private static String getClob(String base) {
        int targetSize = 50 * 1024; // 50KB in bytes
        StringBuilder sb = new StringBuilder(targetSize);
        // Fill with repeated base string until we reach or exceed 50KB
        while (sb.length() * 2 < targetSize) { // *2 because Java chars are 2 bytes
            sb.append(base);
        }
        // Pad with 'X' if needed to reach exactly 50KB
        int remainingChars = (targetSize - sb.length() * 2) / 2;
        for (int i = 0; i < remainingChars; i++) {
            sb.append('X');
        }
        return sb.toString();
    }
}

