import com.mongodb.client.*;
import com.mongodb.client.model.InsertManyOptions;

import org.bson.Document;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class CrashReporterDataLoader {
	
    private static final String CONNECTION_URI =  "<MongoDB-URL>";
    private static final String DATABASE = "CRASH";
    private static final int NUM_DOCS = 1000;
    private static final int BATCH_SIZE = 100;

    private static final List<String> ADOBE_APPS = Arrays.asList(
            "Photoshop", "Illustrator", "Premiere Pro",
            "After Effects", "InDesign", "XD",
            "Lightroom", "Acrobat Pro", "Animate",
            "Dreamweaver"
    );

	public static void main(String[] args) {
		
		try (MongoClient mongoClient = MongoClients.create(CONNECTION_URI)) {
            MongoDatabase db = mongoClient.getDatabase(DATABASE);
            MongoCollection<Document> bucketCollection = db.getCollection("CRD_BUCKET_ADOBE");
            MongoCollection<Document> crashPkgCollection = db.getCollection("CRD_CRASH_PKG");

            Random random = new Random();
            java.util.HashMap<String, Integer> appIdMap = new java.util.HashMap<>();
            int nextAppId = 1000;

            // Calculate the number of unique buckets needed (1 bucket per 10 docs)
            int numBuckets = (int) Math.ceil((double) NUM_DOCS / 10.0);
            int baseBucketId = 11720000;
            java.util.List<Integer> bucketIdList = new java.util.ArrayList<>();
            for (int b = 0; b < numBuckets; b++) {
                bucketIdList.add(baseBucketId + b);
            }

            // Shuffle bucket IDs and ADOBE_APPS for random assignment
            java.util.Collections.shuffle(bucketIdList);
            java.util.List<String> shuffledApps = new java.util.ArrayList<>(ADOBE_APPS);
            java.util.Collections.shuffle(shuffledApps);
            
            // Map each bucket to a specific app (cycle through apps if more buckets than apps)
            java.util.HashMap<Integer, String> bucketToAppMap = new java.util.HashMap<>();
            for (int b = 0; b < numBuckets; b++) {
                String appName = shuffledApps.get(b % shuffledApps.size());
                bucketToAppMap.put(bucketIdList.get(b), appName);
            }

            // Batch processing for better performance
            int batchCount = 0;
            int totalProcessed = 0;
            ArrayList<Document> bucketDocsBatch = new ArrayList<>(BATCH_SIZE);
            ArrayList<Document> crashPkgDocsBatch = new ArrayList<>(BATCH_SIZE);
            
            // Track which buckets have been created
            Set<Integer> createdBuckets = new HashSet<>();
            
            for (int i = 1; i <= NUM_DOCS; i++) {
                // Calculate which bucket this document belongs to
                int bucketIndex = (i - 1) / 10;
                // Get the bucket ID for this document (each bucket will have exactly 10 docs)
                int bucketIdAdobe = bucketIdList.get(bucketIndex);
                // Use the app assigned to this bucket
                String appName = bucketToAppMap.get(bucketIdAdobe);
                int versionMajor = 2024;
                int versionMinor = random.nextInt(10) + 1; // 1 to 10
                String version = versionMajor + "." + versionMinor;
                int buildNum = random.nextInt(10) + 1; // 1 to 10
                String build = String.valueOf(buildNum);

                // Consistent app_id for (appName, version, build) using HashMap
                // In prod this will be done directly in DB
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

                // Create bucket document only once per unique bucket ID
                if (!createdBuckets.contains(bucketIdAdobe)) {
                    // BUCKET DOCUMENT
                    Document bucketDoc = new Document("ID", bucketIdAdobe)
                            .append("MODULE_ID", 200 + bucketIdAdobe)
                            .append("APP", appSubDoc)
                            .append("NAME", "Crash in " + appName)
                            .append("OFFSET", bucketIdAdobe * 1024)
                            .append("BUGNUM", "ADBE-CR-" + bucketIdAdobe)
                            .append("SOLUTION_URL", "https://adobe.com/solution/" + bucketIdAdobe)
                            .append("BUCKET_CREATED", new Date())
                            .append("STATUS", 1)
                            .append("PARENT_BUCKET", null)
                            .append("DEVNOTES", "Issue while using " + appName + " under high load.")
                            .append("CRASH_COUNT", random.nextInt(1000))
                            .append("LAST_CRASH_DATE", new Date())
                            .append("STACK_ELEMENT", "com.adobe." + appName.replaceAll(" ", "") + "::renderFunction")
                            .append("MODULE_NAME", appName)
                            .append("USER_STEPS", "Y")
                            .append("UNIQUE_CRASH_USER_STEPS_COUNT", random.nextInt(200))
                            .append("PLATFORM", "Windows")
                            .append("OS_GROUP_IDS", "WIN11,WIN10")
                            .append("JIRA_ISSUE_IDS", "ADBEJIRA-" + (9000 + bucketIdAdobe));

                    bucketDocsBatch.add(bucketDoc);
                    createdBuckets.add(bucketIdAdobe);
                }

                // CRASH PACKAGE DOCUMENT
                Document crashPkgDoc = new Document("ID", i)
                        .append("UNIQUE_CRASH_ID", 30000 + i)
                        .append("USER_ID", 4000 + i)
                        .append("PLATFORM_ID", 2)
                        .append("MODULE", appName + "Core")
                        .append("OFFSET", 1024 + i)
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
                        .append("BUCKET_ID_ADOBE", bucketIdAdobe)
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
                if (bucketDocsBatch.size() >= BATCH_SIZE || i == NUM_DOCS) {
                    if (!bucketDocsBatch.isEmpty()) {
                        bucketCollection.insertMany(bucketDocsBatch, new InsertManyOptions().ordered(false));
                    }
                    crashPkgCollection.insertMany(crashPkgDocsBatch, new InsertManyOptions().ordered(false));
                    
                    totalProcessed += crashPkgDocsBatch.size();
                    batchCount++;
                    
                    // Print progress
                    System.out.println("Processed batch #" + batchCount + 
                                       " (" + crashPkgDocsBatch.size() + " records)" +
                                       " - Total: " + totalProcessed + "/" + NUM_DOCS);
                    
                    // Clear the batches for next round
                    bucketDocsBatch.clear();
                    crashPkgDocsBatch.clear();
                }
            }

            System.out.println("Dummy crash data inserted into MongoDB.");
        } catch (Exception e) {
            System.err.println("Error inserting data: " + e.getMessage());
            e.printStackTrace();
        }
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
