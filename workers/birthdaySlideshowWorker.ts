import { Job, Worker } from 'bullmq';
import { connection } from '../src/lib/redis';
import { createClient } from '@supabase/supabase-js';
import fetch from 'node-fetch';
import { S3Client, GetObjectCommand, ListObjectsV2Command, ListObjectsV2CommandInput, ListObjectsV2CommandOutput } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import path from 'path';
import { notificationProcessingQueue } from '../src/lib/queues'; // For notifications
export interface BirthdaySlideshowJobData {
  jobId: string;
  userId: string;
  recipientName: string; 
  displayName: string;   
  selectedSongGenre: string; 
  themeRenderUrl: string;    
  message?: string;          
  senderName: string;        
  imageUrls: string[];       
  imageStoragePaths: string[]; 
  imageBucket: string;
  shotstackApiKey?: string;
  shotstackOwnerId?: string;
  // shotstackTemplateId is now dynamic based on genre
  webhookUrl: string;
  myCardId: string; // Added from singingSelfieWorker
  initialThumbnailUrl?: string; // Added from singingSelfieWorker
  bespokeBackgroundUrl?: string; // For AI-generated background
}

interface ShotstackSubmitSuccessResponse {
  success: true;
  message: string;
  response: { id: string; message: string; status: string; };
}

interface ShotstackErrorResponse {
    success: false;
    message: string;
    response?: any;
}

// Added from singingSelfieWorker for polling
interface ShotstackStatusResponse {
    success: boolean;
    message?: string;
    response?: {
        status: 'queued' | 'rendering' | 'done' | 'failed';
        url?: string;
        error?: string;
    };
}

const shotstackSlideshowTemplateMapping: { [key: string]: string } = {
    'Metal': 'd4343b1b-c0f6-4e18-afe6-e37f1f9e3fb8',
    'Punk': '2dde82ce-4dff-4755-8a34-f1e50386e30d',
    'LatinJazz': 'de540b0d-53f9-4437-9212-e3e7c764cb4c',
    'OutlawCountry': 'b5f3bca2-9589-490c-bb06-08199d7959fa',
    'Folk': 'e08fb960-94d8-4871-ac5b-47077e1e9adf',
    'AltPop': '0237998f-9587-45d1-a313-76faad477831',
    'Gospel': 'fadc6d68-3b33-481c-9313-1f05f7878824',
    'JiveBlues': '2361108b-3ec9-482c-8382-db3d4c12b4d5',
    'Jazz': 'b6151c01-fb17-40f3-ad23-40d675f15e45',
    'Reggae': 'cbfa9889-9411-4764-88f9-8decffdf09d4',
    'Pop': '310e6e56-451e-4fc4-8392-780a0ab19d1d',
    'TradJazz': '04a2cd60-2319-4d21-9630-2e3093926573',
    'FolkPop': '98b24939-9e5a-402c-8048-c2f536f43b4b',
    'Classical': '5bf760f0-2f58-4104-8e2d-790c3265bc41',
    'Country': '7196d8dc-b582-404b-b032-7edc8c9702f0',
    'HipHop': '0935cae7-e97d-41d3-b9af-ff583da909cf'
};


// Remove direct Redis instantiation, use imported 'connection'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
const supabaseServiceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseServiceRoleKey) {
  console.error("Supabase URL or Service Role Key is not defined for worker.");
  throw new Error("Supabase URL or Service Role Key is not defined for worker.");
}
const supabaseAdmin = createClient(supabaseUrl, supabaseServiceRoleKey);

const awsKey = process.env.AWS_KEY;
const awsSecret = process.env.AWS_SECRET;
const awsRegion = process.env.AWS_REGION || 'eu-west-2'; 

const s3 = awsKey && awsSecret ? new S3Client({
  region: awsRegion,
  credentials: { accessKeyId: awsKey, secretAccessKey: awsSecret },
}) : null;

if (!s3) {
    console.error('S3 client not initialized in Birthday Slideshow worker. AWS_KEY or AWS_SECRET might be missing.');
}

function convertSpaceToUnderscore(str: string): string {
  return str.replace(/\s+/g, "_");
}
// Helper to normalize genre names for mapping keys (remove spaces)
function normalizeGenreForKey(genreName: string): string {
    return genreName.replace(/\s+/g, '');
}


async function findFileInS3(bucket: string, name: string, genre: string, searchPatternSuffix: string, jobIdForLog: string): Promise<string | null> {
    if (!s3) {
        console.error(`[Worker Job ${jobIdForLog}] S3 client not initialized in worker findFileInS3.`);
        return null;
    }
    if (!bucket) {
        console.error(`[Worker Job ${jobIdForLog}] S3 bucket name not provided to findFileInS3`);
        return null;
    }
    const namefind = convertSpaceToUnderscore(name);
    const processedGenre = normalizeGenreForKey(genre); // Removes spaces
    // Align pattern with singingSelfieWorker: Parentheses around genre
    const pattern = `${namefind}(${processedGenre})${searchPatternSuffix}`;
    
    console.log(`[Worker Job ${jobIdForLog}] Searching S3 bucket '${bucket}' for pattern (includes): '${pattern}'`);
  
    let continuationToken: string | undefined = undefined;
    try {
        do {
            // Remove Prefix to search all objects, like singingSelfieWorker
            const listParams: ListObjectsV2CommandInput = { Bucket: bucket, ContinuationToken: continuationToken };
            const response: ListObjectsV2CommandOutput = await s3.send(new ListObjectsV2Command(listParams));
            
            if (response.Contents) {
                for (const object of response.Contents) {
                    // Match if key includes the pattern, like singingSelfieWorker
                    if (object.Key && object.Key.includes(pattern)) {
                        console.log(`[Worker Job ${jobIdForLog}] Found matching S3 object: ${object.Key}`);
                        const getObjectParams = { Bucket: bucket, Key: object.Key };
                        const command = new GetObjectCommand(getObjectParams);
                         return await getSignedUrl(s3, command, { expiresIn: 3600 }); 
                    }
                }
            }
            continuationToken = response.NextContinuationToken;
        } while (continuationToken);
        console.warn(`[Worker Job ${jobIdForLog}] No S3 object found matching pattern: ${pattern} in bucket ${bucket}`);
        return null;
    } catch (error) {
        console.error(`[Worker Job ${jobIdForLog}] Error searching S3 in findFileInS3 for pattern ${pattern}:`, error);
        return null;
    }
}


async function submitToShotstack(jobData: BirthdaySlideshowJobData): Promise<string> {
  if (!jobData.shotstackApiKey) { // Removed shotstackTemplateId check here
    throw new Error('Shotstack API key is missing in job data.');
  }
  if (!s3) {
    throw new Error('S3 client not initialized, cannot find music file for Shotstack.');
  }

  const normalizedGenreKey = normalizeGenreForKey(jobData.selectedSongGenre);
  const templateId = shotstackSlideshowTemplateMapping[normalizedGenreKey];

  if (!templateId) {
      throw new Error(`No Shotstack template ID found for genre: ${jobData.selectedSongGenre} (Normalized: ${normalizedGenreKey})`);
  }
  console.log(`[Worker Job ${jobData.jobId}] Using Shotstack Template ID: ${templateId} for genre: ${jobData.selectedSongGenre}`);

  // Corrected API endpoint for rendering saved templates
  const apiUrl = `https://api.shotstack.io/edit/v1/templates/render`;
  const musicBucket = 'smhbaudio547';
  const musicSuffix = '_50sec.mp3';

  const musicS3Url = await findFileInS3(musicBucket, jobData.recipientName, jobData.selectedSongGenre, musicSuffix, jobData.jobId);

  if (!musicS3Url) {
    throw new Error(`Could not find music file in S3 for ${jobData.recipientName} - ${jobData.selectedSongGenre}${musicSuffix} in bucket ${musicBucket}`);
  }
  console.log(`[Worker Job ${jobData.jobId}] Using music URL for Shotstack: ${musicS3Url}`);

  let mergeFields = [];

  if (jobData.bespokeBackgroundUrl) {
    console.log(`[Worker Job ${jobData.jobId}] Using bespoke background URL: ${jobData.bespokeBackgroundUrl}`);
    mergeFields = [
      { find: 'name', replace: "" },
      { find: 'greetingbg', replace: jobData.bespokeBackgroundUrl }, // Use bespoke URL
      { find: 'happy', replace: "" },
      { find: 'alth', replace: "" }, // Clear text field
      { find: 'altn', replace: "" }, // Clear text field
      { find: 'message', replace: "" }, // Clear text field
      { find: 'sender', replace: "" }, // Clear text field
      { find: 'music', replace: musicS3Url },
    ];
  } else {
    console.log(`[Worker Job ${jobData.jobId}] Using standard/custom theme render URL: ${jobData.themeRenderUrl}`);
    mergeFields = [
      { find: 'name', replace: "" },
      { find: 'greetingbg', replace: jobData.themeRenderUrl },
      { find: 'happy', replace: "" },
      { find: 'alth', replace: "Happy Birthday" },
      { find: 'altn', replace: jobData.displayName || "" },
      { find: 'message', replace: jobData.message || '' },
      { find: 'sender', replace: jobData.senderName || "" },
      { find: 'music', replace: musicS3Url },
    ];
  }

  // Apply fallback logic for images
  const images = jobData.imageUrls; // Short alias
  const numImages = images.length;

  if (numImages === 0) {
    // This case should ideally not happen if form validation (min 4 images) and API validation works.
    console.error(`[Worker Job ${jobData.jobId}] No images found in jobData.imageUrls. This will likely cause Shotstack to fail.`);
    // To prevent "src" is not allowed to be empty, we must provide something.
    // Using the themeRenderUrl as an emergency fallback for all image slots if no images were provided.
    // This is a safety net; ideally, the API route should prevent jobs with no images.
    for (let i = 1; i <= 7; i++) {
      mergeFields.push({ find: `image${i}`, replace: jobData.themeRenderUrl });
    }
  } else {
    const getImageWithFallback = (primaryImageArrayIndex: number, fallbackImageArrayIndex: number): string => {
      // primaryImageArrayIndex and fallbackImageArrayIndex are 0-based
      if (primaryImageArrayIndex < numImages && images[primaryImageArrayIndex]) {
        return images[primaryImageArrayIndex];
      }
      if (fallbackImageArrayIndex < numImages && images[fallbackImageArrayIndex]) {
        return images[fallbackImageArrayIndex];
      }
      return images[0]; // Ultimate fallback to the very first image
    };
    
    // Form validation ensures 4 to 7 images, so images[0] through images[3] will always exist.
    mergeFields.push({ find: 'image1', replace: images[0] }); // images[0]
    mergeFields.push({ find: 'image2', replace: images[1] }); // images[1]
    mergeFields.push({ find: 'image3', replace: images[2] }); // images[2]
    mergeFields.push({ find: 'image4', replace: images[3] }); // images[3]
  
    // image5: primary is images[4], fallback is images[1] (0-indexed)
    mergeFields.push({ find: 'image5', replace: getImageWithFallback(4, 1) });
    // image6: primary is images[5], fallback is images[2] (0-indexed)
    mergeFields.push({ find: 'image6', replace: getImageWithFallback(5, 2) });
    // image7: primary is images[6], fallback is images[0] (0-indexed)
    mergeFields.push({ find: 'image7', replace: getImageWithFallback(6, 0) });
  }

  // Adjusted body for the /edit/v1/templates/render endpoint
  const body = {
    id: templateId, // Template ID is passed as 'id'
    merge: mergeFields
    // Callback and metadata are typically part of the template's configuration in Shotstack
    // If you need to pass dynamic metadata, it should be done via merge fields if the template supports it.
    // For now, removing callback and metadata from the root, assuming they are set in the template.
  };

  console.log(`[Worker Job ${jobData.jobId}] Submitting to Shotstack. Payload:`, JSON.stringify(body, null, 2));

  let response;
  try {
    response = await fetch(apiUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': jobData.shotstackApiKey!,
        'Accept': 'application/json'
      },
      body: JSON.stringify(body),
    });
  } catch (fetchError: any) {
    console.error(`[Worker Job ${jobData.jobId}] Network error or issue during fetch to Shotstack:`, fetchError);
    throw new Error(`Network error during Shotstack API call: ${fetchError.message}`);
  }

  if (!response.ok) {
    let errorText = 'Unknown error during Shotstack API call.';
    try {
      errorText = await response.text();
    } catch (textError) {
      console.error(`[Worker Job ${jobData.jobId}] Could not get error text from Shotstack response:`, textError);
    }
    console.error(`[Worker Job ${jobData.jobId}] Shotstack API error: ${response.status}`, errorText);
    throw new Error(`Shotstack API request failed: ${response.status} - ${errorText}`);
  }

  let responseData;
  try {
    responseData = await response.json() as ShotstackSubmitSuccessResponse | ShotstackErrorResponse;
  } catch (jsonError: any) {
    console.error(`[Worker Job ${jobData.jobId}] Error parsing JSON response from Shotstack:`, jsonError);
    // Attempt to get text if JSON parsing fails, in case it's not JSON
    let rawResponseText = 'Could not retrieve raw response.';
    try {
        // Note: response.text() can only be consumed once. If it was already consumed by a failed .ok check, this might be empty.
        // However, if .ok was true, but .json() failed, this might give clues.
        // A more robust solution might involve cloning the response if you need to read body multiple times.
        rawResponseText = await response.text(); // This might fail if already read
    } catch (e) { /* ignore */ }
    console.error(`[Worker Job ${jobData.jobId}] Raw response from Shotstack (on JSON parse error):`, rawResponseText);
    throw new Error(`Error parsing JSON response from Shotstack: ${jsonError.message}`);
  }

  if (!responseData.success || !('response' in responseData) || !responseData.response.id) {
    const errorMsg = (responseData as ShotstackErrorResponse).message || 'Shotstack submission did not return a success or render ID.';
    console.error(`[Worker Job ${jobData.jobId}] Shotstack submission failed or malformed response:`, responseData);
    throw new Error(errorMsg);
  }
  
  console.log(`[Worker Job ${jobData.jobId}] Successfully submitted to Shotstack. Render ID: ${(responseData as ShotstackSubmitSuccessResponse).response.id}`);
  return (responseData as ShotstackSubmitSuccessResponse).response.id;
}


const worker = new Worker<BirthdaySlideshowJobData>(
  'birthday-slideshow-processing', // Corrected queue name
  async (job: Job<BirthdaySlideshowJobData>) => {
    const { jobId, userId, shotstackOwnerId } = job.data; // Destructure shotstackOwnerId
    const logPrefix = `[Worker Job ${jobId}]`; // Use logPrefix for consistency
    console.log(`${logPrefix} Processing Birthday Slideshow job for user ${userId}`);

    try {
      if (!supabaseAdmin) {
        throw new Error("Supabase client not initialized in worker.");
      }
      if (!job.data.shotstackApiKey) { // Ensure API key is present before starting
          throw new Error("Shotstack API key missing from job data at worker start.");
      }
      if (!shotstackOwnerId) { // Ensure Owner ID is present
          throw new Error("Shotstack Owner ID missing from job data at worker start.");
      }

      await supabaseAdmin
        .from('my_cards')
        .update({ status: 'processing_slideshow', shotstack_render_id: null, shotstack_status: 'queued' }) // Initial shotstack_status
        .eq('job_id', jobId);
      console.log(`${logPrefix} Status updated to processing_slideshow.`);

      const shotstackRenderId = await submitToShotstack(job.data);
      console.log(`${logPrefix} Submitted to Shotstack. Render ID: ${shotstackRenderId}`);

      await supabaseAdmin
        .from('my_cards')
        .update({ status: 'rendering_slideshow', shotstack_render_id: shotstackRenderId, shotstack_status: 'rendering' }) // Update shotstack_status
        .eq('job_id', jobId);
      console.log(`${logPrefix} Status updated to rendering_slideshow.`);

      // Start Polling Shotstack
      const pollingInterval = 20000; // 20 seconds
      const pollingTimeout = 15 * 60 * 1000; // 15 minutes
      const startTime = Date.now();
      let finalVideoUrl: string | undefined;

      while (Date.now() - startTime < pollingTimeout) {
          await new Promise(resolve => setTimeout(resolve, pollingInterval));
          const statusUrl = `https://api.shotstack.io/edit/v1/render/${shotstackRenderId}`;
          const statusResponse = await fetch(statusUrl, {
              headers: { 'x-api-key': job.data.shotstackApiKey!, 'Accept': 'application/json' },
          });

          if (!statusResponse.ok) {
              console.warn(`${logPrefix} Shotstack status poll failed with status ${statusResponse.status} for ${shotstackRenderId}`);
              continue;
          }
          const statusData = await statusResponse.json() as ShotstackStatusResponse;
          console.log(`${logPrefix} Polling Shotstack status for ${shotstackRenderId}:`, statusData.response?.status);

          if (statusData.success && statusData.response) {
              const currentShotstackStatus = statusData.response.status;
              if (currentShotstackStatus === 'done' && statusData.response.url) {
                  const rawVideoUrl = statusData.response.url;
                  // Construct CDN URL using SHOTSTACK_OWNER_ID
                  const filename = path.basename(rawVideoUrl);
                  finalVideoUrl = `https://cdn.shotstack.io/au/v1/${shotstackOwnerId}/${filename}`;
                  console.log(`${logPrefix} Shotstack render complete. Final CDN URL: ${finalVideoUrl}`);
                  break;
              } else if (currentShotstackStatus === 'failed') {
                  console.error(`${logPrefix} Shotstack render failed: ${statusData.response.error}`);
                  throw new Error(`Shotstack render failed: ${statusData.response.error || 'Unknown Shotstack error'}`);
              }
              // Update shotstack_status in DB if needed (e.g. 'rendering')
              if (currentShotstackStatus !== (await supabaseAdmin.from('my_cards').select('shotstack_status').eq('job_id', jobId).single()).data?.shotstack_status) {
                await supabaseAdmin.from('my_cards').update({ shotstack_status: currentShotstackStatus }).eq('job_id', jobId);
              }
          } else {
              console.warn(`${logPrefix} Error polling Shotstack status or unexpected response:`, statusData.message);
          }
      }

      if (!finalVideoUrl) {
          await supabaseAdmin
            .from('my_cards')
            .update({ status: 'failed', shotstack_status: 'timeout', error_message: 'Shotstack polling timed out.' })
            .eq('job_id', jobId);
          console.error(`${logPrefix} Shotstack polling timed out.`);
          throw new Error('Shotstack polling timed out.');
      }

      await supabaseAdmin
          .from('my_cards')
          .update({ video_url: finalVideoUrl, status: 'complete', shotstack_status: 'complete' })
          .eq('job_id', jobId);
      console.log(`${logPrefix} Successfully updated my_cards with final video URL and status COMPLETE.`);

      // --- Create public share link and update my_cards (adapted from singingSelfieWorker) ---
      let finalShortLinkeCardUrl = `${process.env.NEXT_PUBLIC_SITE_URL || 'http://localhost:3000'}`; // Default fallback

      if (job.data.myCardId && shotstackRenderId) { // shotstackRenderId is available from earlier
        console.log(`${logPrefix} Attempting to insert into public_shared_cards for myCardId: ${job.data.myCardId}`);
        const { data: publicInsertData, error: publicInsertError } = await supabaseAdmin
            .from('public_shared_cards')
            .insert([{
                my_card_id: job.data.myCardId,
                thumbnail_url: job.data.initialThumbnailUrl, // Use thumbnail from jobData
            }])
            .select('id')
            .single();

        if (publicInsertError) {
            console.error(`${logPrefix} Error inserting into public_shared_cards:`, publicInsertError);
        } else if (publicInsertData) {
            const publicCardId = publicInsertData.id;
            console.log(`${logPrefix} Successfully inserted public link ${publicCardId} for myCardId: ${job.data.myCardId}`);
            
            // Construct short link. Ensure renderId is relevant for slideshows or adjust as needed.
            const constructedShortLink = `/ecard/view/${publicCardId}?renderId=${shotstackRenderId}`;
            const { error: updateLinkError } = await supabaseAdmin
                .from('my_cards')
                .update({ shortLinkeCard: constructedShortLink })
                .eq('id', job.data.myCardId);

            if (updateLinkError) {
                console.error(`${logPrefix} Error updating my_cards with shortLinkeCard:`, updateLinkError);
            } else {
                finalShortLinkeCardUrl = `${process.env.NEXT_PUBLIC_SITE_URL || 'http://localhost:3000'}${constructedShortLink}`;
                console.log(`${logPrefix} Successfully updated my_cards with shortLinkeCard: ${finalShortLinkeCardUrl}`);
            }
        } else {
            console.error(`${logPrefix} Insert into public_shared_cards returned no data or ID.`);
        }
      } else {
        console.warn(`${logPrefix} Missing myCardId or shotstackRenderId, cannot create full shortLinkeCard.`);
      }

      // --- Send Notification (adapted from singingSelfieWorker) ---
      // Use relevant names for the slideshow context
      const cardTitle = job.data.displayName || job.data.recipientName || "your slideshow";

      await notificationProcessingQueue.add('card-ready', {
        userId: userId, type: 'card_ready',
        message: `Your Birthday Slideshow "${cardTitle}" is ready! Click here to view.`,
        link: finalShortLinkeCardUrl,
      });
      console.log(`${logPrefix} Card ready notification job added to queue for user ${userId}.`);

    } catch (error: any) {
      console.error(`${logPrefix} Error processing job:`, error);
      if (supabaseAdmin) {
        await supabaseAdmin
          .from('my_cards')
          .update({ status: 'failed', error_message: error.message })
          .eq('job_id', jobId);
      }
      throw error; 
    }
  },
  { 
    connection,
    concurrency: parseInt(process.env.SLIDESHOW_WORKER_CONCURRENCY || "5"),
    removeOnComplete: { count: 1000, age: 24 * 3600 }, 
    removeOnFail: { count: 5000, age: 7 * 24 * 3600 }    
  }
);

worker.on('completed', (job: Job<BirthdaySlideshowJobData>) => {
  console.log(`[Worker Job ${job.data.jobId}] Completed successfully.`);
});

worker.on('failed', (job: Job<BirthdaySlideshowJobData> | undefined, error: Error) => {
  if (job) {
    console.error(`[Worker Job ${job.data.jobId}] Failed with error: ${error.message}`, error.stack);
  } else {
    console.error(`Worker failed a job with no job data available. Error: ${error.message}`, error.stack);
  }
});

console.log("Birthday Slideshow Worker started.");