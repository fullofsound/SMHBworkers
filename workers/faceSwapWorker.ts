import { Worker, Job } from 'bullmq';
import { connection } from '../src/lib/redis'; // Use relative path
import fetch from 'node-fetch'; // Use node-fetch for fetching external URLs
import { Buffer } from 'buffer'; // Import Buffer for base64 conversion
import fs from 'fs/promises'; // Import fs promises for async file reading
import path from 'path'; // Import path for resolving file paths
import { parse } from 'csv-parse/sync'; // Import sync parser for simplicity in worker
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3'; // Import AWS S3 Client
import { createClient } from '@supabase/supabase-js'; // Import Supabase client
import { notificationProcessingQueue } from '../src/lib/queues'; // Import the notification queue


// Define interface for CSV template data
interface TemplateData {
    ID: string;
    Name: string;
    Image: string;
    RenderImage: string;
}

// Helper function to fetch an image from a URL and convert it to base64
async function imageUrlToBase64(imageUrl: string): Promise<string> {
    const response = await fetch(imageUrl);
    if (!response.ok) {
        throw new Error(`Failed to fetch image from URL: ${imageUrl} status: ${response.status}`);
    }
    const arrayBuffer = await response.arrayBuffer();
    return Buffer.from(arrayBuffer).toString('base64');
}


new Worker(
  'face-swap',
  async (job: Job) => {
// --- Environment Variable Check (inside worker callback) ---
    // Use variable names from .env.local
    const segmindApiKey = process.env.SEGMIND_API_KEY;
    const siteUrl = process.env.NEXT_PUBLIC_SITE_URL;
    const awsKey = process.env.AWS_KEY;
    const awsSecret = process.env.AWS_SECRET;
    const awsRegion = process.env.AWS_REGION; // Assuming AWS_REGION is still correct, otherwise update
    const awsS3BucketUser = process.env.AWS_BUCKET_USER;
    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL; // Corrected variable name
    const supabaseServiceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
    const segmindTimeoutMs = parseInt(process.env.SEGMIND_API_TIMEOUT_MS || '90000', 10); // New environment variable for timeout
    const segmindApiEndpointUrl = process.env.SEGMIND_API_ENDPOINT_URL; // New environment variable for custom endpoint

    if (!segmindApiKey || !siteUrl || !awsKey || !awsSecret || !awsRegion || !awsS3BucketUser || !supabaseUrl || !supabaseServiceRoleKey || !segmindApiEndpointUrl) {
        console.error(`Job ${job.id}: Missing required environment variables (SEGMIND_API_KEY, NEXT_PUBLIC_SITE_URL, AWS_KEY, AWS_SECRET, AWS_REGION, AWS_BUCKET_USER, NEXT_PUBLIC_SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, SEGMIND_API_ENDPOINT_URL).`);
        throw new Error('Missing required environment variables for job execution.');
    }

    // --- Initialize Clients (inside worker callback) ---
    const s3Client = new S3Client({
        region: awsRegion, // Assuming AWS_REGION is correct
        credentials: {
            accessKeyId: awsKey,
            secretAccessKey: awsSecret,
        },
    });

    const supabaseAdmin = createClient(supabaseUrl, supabaseServiceRoleKey, {
        auth: {
            // Required for service role key
            autoRefreshToken: false,
            persistSession: false
        }
    });


    if (job.name === 'generate') {
      const { imageUrl, templateId, userId } = job.data; // Assuming userId is passed in job data

      if (!userId) {
          console.error(`Job ${job.id} missing userId.`);
          throw new Error('User ID is required for faceswap job.');
      }

      // --- Fetch template RenderImage URL from CSV ---
      let templateRenderImageUrl: string;
      try {
        const csvPath = path.resolve(__dirname, '../../public/faceswap_templates.csv'); // Adjust path relative to dist-worker/workers
        const csvData = await fs.readFile(csvPath, 'utf-8');
        const records: TemplateData[] = parse(csvData, {
          columns: true, // Use headers as keys
          skip_empty_lines: true,
        });

        const templateRecord = records.find(record => record.ID === String(templateId)); // Find matching ID (ensure type match)

        if (!templateRecord || !templateRecord.RenderImage) {
          throw new Error(`Template ID ${templateId} not found or missing RenderImage in CSV.`);
        }

        // Using templateRecord.RenderImage (column 4) as the base for the URL

        const siteUrl = process.env.NEXT_PUBLIC_SITE_URL;
        if (!siteUrl) {
            throw new Error('NEXT_PUBLIC_SITE_URL environment variable is not set.');
        }
        // Construct full URL: siteUrl + relative path from CSV
        templateRenderImageUrl = new URL(templateRecord.RenderImage, siteUrl).toString();
        console.log(`Found template URL for ID ${templateId}: ${templateRenderImageUrl}`);

      } catch (csvError) {
        console.error('Error reading or parsing faceswap_templates.csv:', csvError);
        throw new Error('Failed to load template data.'); // Fail the job if template can't be found
      }
      // --- End Fetch template RenderImage URL ---


      // Define interface for Segmind API response
      interface SegmindFaceswapResponse {
          image: string; // Base64 image data
          // Add other potential properties if needed
      }

      // 1) Call Segmind API for faceswap
      // Use the environment variable for the API URL. The check above ensures it's defined.
      const segmindApiUrl = segmindApiEndpointUrl!;

      let timeoutId: NodeJS.Timeout | null = null; // Declare timeoutId outside try block
      try {
        // Convert images to base64
        console.log(`Job ${job.id}: Starting to fetch and convert source image from ${imageUrl}`);
        const sourceStartTime = Date.now();
        const sourceImageBase64 = await imageUrlToBase64(imageUrl);
        const sourceEndTime = Date.now();
        console.log(`Job ${job.id}: Source image processed in ${(sourceEndTime - sourceStartTime) / 1000}s. Base64 size: ${sourceImageBase64.length} bytes.`);

        console.log(`Job ${job.id}: Starting to fetch and convert target image from ${templateRenderImageUrl}`);
        const targetStartTime = Date.now();
        const targetImageBase64 = await imageUrlToBase64(templateRenderImageUrl);
        const targetEndTime = Date.now();
        console.log(`Job ${job.id}: Target image processed in ${(targetEndTime - targetStartTime) / 1000}s. Base64 size: ${targetImageBase64.length} bytes.`);

        // --- Add Timeout Controller ---
        const controller = new AbortController();
        timeoutId = setTimeout(() => { // Assign to the outer timeoutId
            console.log(`Job ${job.id}: Segmind API call timed out after ${segmindTimeoutMs / 1000} seconds.`);
            controller.abort();
        }, segmindTimeoutMs); // Use configurable timeout

        console.log(`Job ${job.id}: Calling Segmind API with source: ${imageUrl}, target: ${templateRenderImageUrl}`); // Added image URLs for context
        console.log(`Job ${job.id}: Timestamp before Segmind fetch: ${new Date().toISOString()}`);
        const segmindResponse = await fetch(segmindApiUrl, {
          method: 'POST',
          signal: controller.signal, // Add abort signal
          headers: {
            'Content-Type': 'application/json',
            'x-api-key': segmindApiKey, // Use non-null assertion as checked above
          },
          body: JSON.stringify({
            source_image: sourceImageBase64, // Use base64 source image
            target_image: targetImageBase64, // Use base64 target image
            model_type: "speed", // Assuming speed model is desired
            swap_type: "face", // Changed swap_type to "face"
            style_type: "normal", // Assuming normal style is desired
            image_format: "jpg", // Request JPG format
            image_quality: 50, // Request 50% quality
            base64: true // Request base64 output
          }),
        });

        clearTimeout(timeoutId); // Clear the timeout if fetch completes
        console.log(`Job ${job.id}: Timestamp after Segmind fetch: ${new Date().toISOString()}`);

        console.log(`Segmind API response status for job ${job.id}: ${segmindResponse.status}`); // Log status code

        if (!segmindResponse.ok) {
          let errorBody = 'Could not read error body.';
          try {
              errorBody = await segmindResponse.text();
          } catch (readError) {
              console.error(`Job ${job.id}: Failed to read Segmind error body:`, readError);
          }
          console.error(`Segmind API error for job ${job.id}: Status ${segmindResponse.status}, Body: ${errorBody}`);
          throw new Error(`Segmind API request failed with status ${segmindResponse.status}`);
        }

        console.log(`Segmind API call successful for job ${job.id}. Parsing JSON response...`);
        const segmindResult = await segmindResponse.json() as SegmindFaceswapResponse; // Explicitly cast the result
        console.log(`Segmind API JSON parsed successfully for job ${job.id}.`);

        const resultBase64 = segmindResult.image;

        if (!resultBase64) {
            console.error(`Segmind API response missing image data for job ${job.id}.`);
            throw new Error('Faceswap failed: No image data returned.');
        }

        // 2) Decode base64 and prepare for S3 upload
        const imageBuffer = Buffer.from(resultBase64, 'base64');
        const s3Key = `user_faceswaps/${userId}/${job.id}.jpg`; // Define S3 path

        // 3) Upload to S3
        console.log(`Uploading faceswap image to S3 for job ${job.id} at key: ${s3Key}`);
        const putObjectCommand = new PutObjectCommand({
            Bucket: awsS3BucketUser, // Use correct bucket name variable
            Key: s3Key,
            Body: imageBuffer,
            ContentType: 'image/jpeg', // Set appropriate content type
            CacheControl: 'public, max-age=31536000, immutable', // Add this line
            // ACL: 'private', // Or your desired ACL, bucket policy is often preferred
        });

        try {
            await s3Client.send(putObjectCommand);
            console.log(`S3 upload successful for job ${job.id}.`);
        } catch (s3Error) {
            console.error(`S3 upload failed for job ${job.id}:`, s3Error);
            throw new Error('Failed to upload faceswap image to S3.'); // Fail the job
        }

        // 4) Insert record into Supabase
        console.log(`Inserting faceswap record into Supabase for job ${job.id}.`);
        const { data: insertData, error: insertError } = await supabaseAdmin
            .from('my_faceswaps')
            .insert({
                user_id: userId,
                s3_path: s3Key,
                source_image_url: imageUrl, // Store original source URL
                template_image_url: templateRenderImageUrl, // Store template URL used
            })
            .select('id') // Optionally select the new ID if needed later
            .single(); // Expecting a single row insertion

        if (insertError) {
            console.error(`Supabase insert failed for job ${job.id}:`, insertError);
            // Consider if you need to delete the S3 object if DB insert fails (compensating transaction)
            throw new Error('Failed to save faceswap record to database.'); // Fail the job
        }
        console.log(`Supabase insert successful for job ${job.id}. New record ID: ${insertData?.id}`);

        // Dispatch notification job
        if (insertData?.id) { // Ensure we have a card ID to link to
          const siteUrl = process.env.NEXT_PUBLIC_SITE_URL || 'http://localhost:3000'; // Fallback URL
          const notificationLink = `${siteUrl}/profile?faceswapId=${insertData.id}`;

          await notificationProcessingQueue.add('faceswap-complete', {
            userId: userId,
            type: 'faceswap_complete',
            message: `Your faceswap is ready! Click here to view.`,
            link: notificationLink,
          });
          console.log(`Notification job dispatched for faceswap ${insertData.id} for user ${userId}`);
        } else {
          console.warn(`Job ${job.id}: Could not dispatch notification as faceswap ID was not available from insertData.`);
        }

        // 5) Update job data in Redis: Remove base64, add s3_path
        // Construct a new data object for the update
        let newJobData = {
          ...job.data, // Spread existing data
          s3_path: s3Key, // Add/overwrite s3_path
          databaseId: insertData?.id || null // Add/overwrite databaseId, explicitly null if not present
        };
        delete newJobData.resultBase64; // Remove the large base64 string from the new object

        console.log(`[worker] Job ${job.id} - newJobData object before updateData call:`, JSON.stringify(newJobData)); // DEBUG
        await job.updateData(newJobData); // Keep this for direct job data inspection if needed
        console.log(`Redis job data updated for job ${job.id}: with s3_path and databaseId.`);

        console.log(`Faceswap job ${job.id} completed successfully and saved.`);
        
        // Return the essential data for the status API
        const jobReturnValue = {
          s3_path: s3Key,
          databaseId: insertData?.id || null
        };
        console.log(`[worker] Job ${job.id} - Returning:`, JSON.stringify(jobReturnValue));
        return jobReturnValue; // This will be stored in job.returnvalue

      } catch (error) {
        console.error(`Error processing faceswap job ${job.id}:`, error);
        // BullMQ will automatically mark the job as failed if an error is thrown
        // Ensure any intermediate state (like S3 upload without DB record) is handled if necessary
        if (timeoutId) clearTimeout(timeoutId); // Ensure timeout is cleared on error too, check if not null
        // Check if the error was due to the timeout
        if (error instanceof Error && error.name === 'AbortError') {
             throw new Error(`Segmind API call timed out for job ${job.id}.`);
        }
        throw error; // Re-throw other errors
      }

    }
  },
  {
    connection,
    // Add retry strategies here if needed
    // attempts: 3,
    // backoff: {
    //   type: 'exponential',
    //   delay: 1000,
    // },
  }
);

console.log('Faceswap worker started with S3 and Supabase integration.');