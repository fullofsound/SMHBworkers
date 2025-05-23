// workers/singingSelfieWorker.ts
import { Worker, Job } from 'bullmq';
import { connection } from '../src/lib/redis';
import { singingSelfieQueue } from '../src/lib/queues';
import { supabaseAdmin } from '../src/lib/supabaseAdmin';
import { notificationProcessingQueue } from '../src/lib/queues'; // For dispatching card_ready notifications
import { NotificationJobData } from './notificationWorker'; // Import for failure notifications
import path from 'path';
import { S3Client, GetObjectCommand, PutObjectCommand, ListObjectsV2Command, ListObjectsV2CommandInput, ListObjectsV2CommandOutput } from "@aws-sdk/client-s3"; // Added ListObjectsV2CommandInput, ListObjectsV2CommandOutput
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import fetch from 'node-fetch';
import { Buffer } from 'buffer';

// TODO: Move these interfaces to a shared types file if not already there
interface ShotstackRenderResponse {
    success: boolean;
    message?: string;
    response?: {
        id: string;
        message?: string;
    };
}

interface ShotstackStatusResponse {
    success: boolean;
    message?: string;
    response?: {
        status: 'queued' | 'rendering' | 'done' | 'failed';
        url?: string;
        error?: string;
    };
}

interface HedraAsset {
    id: string;
    name: string;
    type: 'image' | 'audio' | 'video';
}

interface HedraGenerationStatus {
    status: 'pending' | 'processing' | 'complete' | 'error';
    url?: string;
    error_message?: string;
    asset_id?: string;
}

interface HedraGenerationResponse {
    id: string;
    status: string;
    created_at: string;
    updated_at: string;
    type: string;
    ai_model_id: string;
    start_keyframe_id: string;
    audio_id: string;
    generated_video_inputs: {
        text_prompt: string;
        resolution: string;
        aspect_ratio: string;
        duration_ms: number;
        seed: number;
    };
    error_message: string | null;
    url: string | null;
}

// Constants
const HEDRA_API_BASE_URL = "https://api.hedra.com/web-app/public";
const HEDRA_API_KEY = process.env.HEDRA_API_KEY;
const SHOTSTACK_API_KEY = process.env.SHOTSTACK_API_KEY;
const SHOTSTACK_OWNER_ID = process.env.SHOTSTACK_OWNER_ID;

const shotstackTemplateMapping: { [key: string]: string } = {
    'Metal': '53d55f74-8a8a-4c34-bc2b-c0abfb6d1e2a',
    'Punk': '9a6c2d30-384c-4087-b9f1-ced55aef2809',
    'LatinJazz': '24b4423d-f2d9-4f72-b7c7-94307732619f',
    'OutlawCountry': 'e948f56c-12df-4438-86e6-c96dc2e59035',
    'Folk': '8e9a90b6-1d6e-46b5-9c07-c35c7325c83c',
    'AltPop': 'f3e830e3-758b-4873-b572-fceacf4a2f21',
    'Gospel': 'ce86bafa-151b-4d3f-b755-5f6b32c644fd',
    'JiveBlues': '3543ed64-8556-4745-ae90-3c5e754a578b',
    'Jazz': 'b14a681e-eba1-4557-9082-ea5308f14774',
    'Reggae': '9e5217ff-21b4-4086-8f1b-1c60691cd2ed',
    'Pop': '019e78b2-7bc3-4f5e-a560-9c2a8964f07b', // Test ID
    'TradJazz': '0e170786-99b5-498d-859a-c7a806c8290c',
    'FolkPop': '29f0faa8-597e-43ad-966c-94c2d4bae269',
    'Classical': '1e9552c2-be82-42cb-8b2e-4f4aa7e4ef43',
    'Country': '71f2a9c6-bfdf-446c-83cb-69786b474023',
    'HipHop': '32c13059-ff08-4c89-86b9-dd855cc6acc2',
};

const awsKey = process.env.AWS_KEY;
const awsSecret = process.env.AWS_SECRET;
const awsRegion = process.env.AWS_REGION;

const s3 = awsKey && awsSecret && awsRegion ? new S3Client({
  region: awsRegion,
  credentials: {
    accessKeyId: awsKey,
    secretAccessKey: awsSecret,
  },
}) : null;

function convertSpaceToUnderscore(str: string): string {
  return str.replace(/\s+/g, "_");
}

async function findFileInS3(bucket: string, name: string, genre: string, searchPatternSuffix: string): Promise<string | null> {
    if (!s3) {
        console.error('S3 client not initialized in worker findFileInS3.');
        return null;
    }
    if (!bucket) {
        console.error('S3 bucket name not provided to findFileInS3');
        return null;
    }
    const namefind = convertSpaceToUnderscore(name);
    const processedGenre = genre.replace(/\s/g, ''); // Remove spaces for matching
    const pattern = `${namefind}(${processedGenre})${searchPatternSuffix}`;
    let continuationToken: string | undefined = undefined;
    try {
        do {
            const listParams: ListObjectsV2CommandInput = { Bucket: bucket, ContinuationToken: continuationToken };
            const response: ListObjectsV2CommandOutput = await s3.send(new ListObjectsV2Command(listParams));
            if (response.Contents) {
                for (const object of response.Contents) {
                    if (object.Key && object.Key.includes(pattern)) {
                        const getObjectParams = { Bucket: bucket, Key: object.Key };
                        const command = new GetObjectCommand(getObjectParams);
                        return await getSignedUrl(s3, command, { expiresIn: 1200 }); // 20 minutes
                    }
                }
            }
            continuationToken = response.NextContinuationToken;
        } while (continuationToken);
        console.log(`Worker: No S3 object found matching pattern: ${pattern} in bucket ${bucket}`);
        return null;
    } catch (error) {
        console.error('Worker: Error searching S3 in findFileInS3:', error);
        return null;
    }
}

async function pollHedraGenerationStatus(fullGenerationId: string): Promise<string | null> {
    const uuidMatch = fullGenerationId.match(/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/);
    const generationId = uuidMatch ? uuidMatch[0] : null;
    if (!generationId) {
        console.error("Worker: Could not extract valid UUID from Hedra generation ID:", fullGenerationId);
        return null;
    }
    const statusUrl = `${HEDRA_API_BASE_URL}/generations/${generationId}/status`;
    console.log("Worker: Polling Hedra URL:", statusUrl);
    let attempts = 0;
    const maxAttempts = 30; // Poll for 10 minutes (30 * 20s)
    while (attempts < maxAttempts) {
        attempts++;
        const response = await fetch(statusUrl, { headers: { 'x-api-key': HEDRA_API_KEY as string } });
        if (!response.ok) {
            console.error(`Worker: Hedra status poll failed with status ${response.status} for ${generationId}`);
            await new Promise(resolve => setTimeout(resolve, 20000));
            continue;
        }
        const data = await response.json() as HedraGenerationStatus;
        console.log("Worker: Polling Hedra status for", generationId, ":", data.status);
        if (data.status === 'complete' && data.url) return data.url;
        if (data.status === 'error') {
            console.error('Worker: Hedra API generation failed:', data.error_message);
            return null;
        }
        await new Promise(resolve => setTimeout(resolve, 20000)); // 20 seconds
    }
    console.error(`Worker: Hedra polling timed out after ${maxAttempts} attempts for ${generationId}`);
    return null;
}

export interface SingingSelfieJobData {
  jobId: string; // Unique ID generated by the API route before queueing
  userId: string;
  sourceImageUrl: string;
  inputImageName: string;
  textPrompt: string;
  aspectRatio: string;
  resolution: string;
  durationMs?: number; // Optional, for Hedra
  seed?: number;      // Optional, for Hedra
  songName: string;
  displayName: string;
  message?: string;
  yourName: string;
  themeRenderUrl: string;
  genre: string;
  initialThumbnailUrl?: string; // Passed from API if thumbnail was pre-generated
  myCardId: string; // The ID of the row in the my_cards table
  bespokeBackgroundUrl?: string; // For AI-generated background
}

console.log('Singing Selfie worker starting, connecting to queue:', singingSelfieQueue.name);

const singingSelfieWorkerImpl = new Worker<SingingSelfieJobData>(
  singingSelfieQueue.name,
  async (job: Job<SingingSelfieJobData>) => {
    const {
      jobId, // This is our unique ID for the task
      userId,
      sourceImageUrl,
      inputImageName,
      textPrompt,
      aspectRatio,
      resolution,
      durationMs,
      seed,
      songName,
      displayName,
      message,
      yourName,
      themeRenderUrl,
      genre,
      initialThumbnailUrl,
      bespokeBackgroundUrl, // Destructure the new field
    } = job.data;

    const logPrefix = `[Job ${jobId}]`;
    console.log(`${logPrefix} Processing Singing Selfie for user ${userId}, song: ${songName}`);

    if (!HEDRA_API_KEY || !SHOTSTACK_API_KEY || !SHOTSTACK_OWNER_ID) {
        console.error(`${logPrefix} Missing API keys (Hedra or Shotstack). Aborting.`);
        throw new Error('Missing API keys in worker environment.');
    }
    if (!s3) {
        console.error(`${logPrefix} S3 client not initialized. Aborting.`);
        throw new Error('S3 client not initialized in worker.');
    }
    if (!supabaseAdmin) {
        console.error(`${logPrefix} Supabase admin client not initialized. Aborting.`);
        throw new Error('Supabase admin client not initialized in worker.');
    }

    try {
      // Ensure status is PROCESSING_ASSETS when worker starts
      const { error: initialStatusUpdateError } = await supabaseAdmin
        .from('my_cards')
        .update({ status: 'PROCESSING_ASSETS' })
        .eq('job_id', jobId);
      if (initialStatusUpdateError) console.warn(`${logPrefix} Failed to update initial status to PROCESSING_ASSETS: ${initialStatusUpdateError.message}`);
      else console.log(`${logPrefix} Status updated to PROCESSING_ASSETS`);

      console.log(`${logPrefix} Fetching source image from: ${sourceImageUrl}`);
      const imageResponse = await fetch(sourceImageUrl);
      if (!imageResponse.ok) {
          throw new Error(`Failed to fetch source image ${sourceImageUrl}: ${imageResponse.statusText}`);
      }
      const imageArrayBuffer = await imageResponse.arrayBuffer();
      const imageInputBuffer = Buffer.from(imageArrayBuffer);

      const hedraAudioBucket = process.env.AWS_BUCKET_HEDRA_AUDIO as string;
      const hedraAudioSuffix = '_30sec_hedra.mp3';
      console.log(`${logPrefix} Finding Hedra audio in S3: bucket=${hedraAudioBucket}, song=${songName}, genre=${genre}`);
      const hedraAudioS3Url = await findFileInS3(hedraAudioBucket, songName, genre, hedraAudioSuffix);
      if (!hedraAudioS3Url) {
          throw new Error(`Hedra audio file not found in S3 for song: ${songName}, genre: ${genre}`);
      }
      console.log(`${logPrefix} Fetching Hedra audio from S3 URL: ${hedraAudioS3Url}`);
      const audioS3Response = await fetch(hedraAudioS3Url);
      if (!audioS3Response.ok) {
          throw new Error(`Failed to fetch Hedra audio from S3 ${hedraAudioS3Url}: ${audioS3Response.statusText}`);
      }
      const audioHedraArrayBuffer = await audioS3Response.arrayBuffer();
      const audioBlobForHedra = new Blob([audioHedraArrayBuffer], { type: 'audio/mpeg' });
      const audioFileNameForHedra = `${convertSpaceToUnderscore(songName)}(${convertSpaceToUnderscore(genre)})${hedraAudioSuffix}`;

      // Update status before starting Hedra API calls
      const { error: hedraStatusUpdateError } = await supabaseAdmin
        .from('my_cards')
        .update({ status: 'GENERATING_AI_VIDEO' })
        .eq('job_id', jobId);
      if (hedraStatusUpdateError) console.warn(`${logPrefix} Failed to update status to GENERATING_AI_VIDEO: ${hedraStatusUpdateError.message}`);
      else console.log(`${logPrefix} Status updated to GENERATING_AI_VIDEO`);

      console.log(`${logPrefix} Starting Hedra processing...`);
      // Log the API base URL and a portion of the API key for verification
      console.log(`${logPrefix} Hedra API Base URL: ${HEDRA_API_BASE_URL}`);
      console.log(`${logPrefix} Hedra API Key (first 5 chars): ${HEDRA_API_KEY?.substring(0, 5)}...`);

      const modelsResponse = await fetch(`${HEDRA_API_BASE_URL}/models`, { headers: { 'x-api-key': HEDRA_API_KEY! } });

      // Log response status
      console.log(`${logPrefix} Hedra /models response status: ${modelsResponse.status}`);
      console.log(`${logPrefix} Hedra /models response ok: ${modelsResponse.ok}`);

      if (!modelsResponse.ok) {
          const errorText = await modelsResponse.text();
          console.error(`${logPrefix} Hedra /models API request failed with status ${modelsResponse.status}: ${errorText}`);
          throw new Error(`Hedra /models API request failed: ${modelsResponse.statusText}`);
      }

      const modelsData = await modelsResponse.json() as any[];
      // Log the raw modelsData
      console.log(`${logPrefix} Hedra /models response data: ${JSON.stringify(modelsData, null, 2)}`);

      const modelId = modelsData?.[0]?.id; // Using optional chaining for modelsData as well
      if (!modelId) {
          console.error(`${logPrefix} Could not retrieve Hedra model ID from data: ${JSON.stringify(modelsData, null, 2)}`);
          throw new Error('Could not retrieve Hedra model ID');
      }
      console.log(`${logPrefix} Using Hedra model ID: ${modelId}`);

      const createImageAssetResponse = await fetch(`${HEDRA_API_BASE_URL}/assets`, {
          method: 'POST',
          headers: { 'x-api-key': HEDRA_API_KEY!, 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: inputImageName, type: 'image' }),
      });
      if (!createImageAssetResponse.ok) throw new Error(`Failed to create Hedra image asset: ${await createImageAssetResponse.text()}`);
      const imageAssetData = await createImageAssetResponse.json() as HedraAsset;
      const imageAssetId = imageAssetData.id;
      console.log(`${logPrefix} Created Hedra image asset: ${imageAssetId}`);

      const imageUploadFormData = new FormData();
      imageUploadFormData.append('file', new Blob([imageInputBuffer]), inputImageName);
      const imageUploadResponse = await fetch(`${HEDRA_API_BASE_URL}/assets/${imageAssetId}/upload`, {
          method: 'POST', headers: { 'x-api-key': HEDRA_API_KEY! }, body: imageUploadFormData,
      });
      if (!imageUploadResponse.ok) throw new Error(`Hedra image upload failed: ${await imageUploadResponse.text()}`);
      console.log(`${logPrefix} Uploaded image to Hedra asset: ${imageAssetId}`);

      const createAudioAssetResponse = await fetch(`${HEDRA_API_BASE_URL}/assets`, {
          method: 'POST',
          headers: { 'x-api-key': HEDRA_API_KEY!, 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: audioFileNameForHedra, type: 'audio' }),
      });
      if (!createAudioAssetResponse.ok) throw new Error(`Failed to create Hedra audio asset: ${await createAudioAssetResponse.text()}`);
      const audioAssetData = await createAudioAssetResponse.json() as HedraAsset;
      const audioAssetId = audioAssetData.id;
      console.log(`${logPrefix} Created Hedra audio asset: ${audioAssetId}`);

      const audioUploadFormData = new FormData();
      audioUploadFormData.append('file', audioBlobForHedra, audioFileNameForHedra);
      const audioUploadResponse = await fetch(`${HEDRA_API_BASE_URL}/assets/${audioAssetId}/upload`, {
          method: 'POST', headers: { 'x-api-key': HEDRA_API_KEY! }, body: audioUploadFormData,
      });
      if (!audioUploadResponse.ok) throw new Error(`Hedra audio upload failed: ${await audioUploadResponse.text()}`);
      console.log(`${logPrefix} Uploaded audio to Hedra asset: ${audioAssetId}`);

      const hedraGenInputs: any = { text_prompt: textPrompt, resolution, aspect_ratio: aspectRatio };

      const generationRequestData = {
          type: 'video', ai_model_id: modelId, start_keyframe_id: imageAssetId, audio_id: audioAssetId,
          generated_video_inputs: hedraGenInputs,
      };
      const submitGenerationResponse = await fetch(`${HEDRA_API_BASE_URL}/generations`, {
          method: 'POST',
          headers: { 'x-api-key': HEDRA_API_KEY!, 'Content-Type': 'application/json' },
          body: JSON.stringify(generationRequestData),
      });
      if (!submitGenerationResponse.ok) throw new Error(`Failed to submit Hedra generation job: ${await submitGenerationResponse.text()}`);
      const generationResponseData = await submitGenerationResponse.json() as HedraGenerationResponse;
      const hedraGenerationId = generationResponseData.id;
      console.log(`${logPrefix} Submitted Hedra generation job: ${hedraGenerationId}`);

      const hedraVideoUrl = await pollHedraGenerationStatus(hedraGenerationId);
      if (!hedraVideoUrl) throw new Error(`Hedra video generation failed or timed out for Hedra job ${hedraGenerationId}`);
      console.log(`${logPrefix} Hedra video generated: ${hedraVideoUrl}`);

      // Update status before starting Shotstack processing
      const { error: compositingStatusUpdateError } = await supabaseAdmin
        .from('my_cards')
        .update({
          status: 'COMPOSITING_FINAL_VIDEO',
          hedra_video_url: hedraVideoUrl,
          shotstack_status: 'queued'
        })
        .eq('job_id', jobId);
      if (compositingStatusUpdateError) console.warn(`${logPrefix} Failed to update status to COMPOSITING_FINAL_VIDEO: ${compositingStatusUpdateError.message}`);
      else console.log(`${logPrefix} Status updated to COMPOSITING_FINAL_VIDEO`);

      console.log(`${logPrefix} Starting Shotstack processing...`);
      const shotstackMusicBucket = process.env.AWS_BUCKET_AUDIO as string;
      const shotstackMusicSuffix = '_30sec.mp3';
      console.log(`${logPrefix} Finding Shotstack music in S3: bucket=${shotstackMusicBucket}, song=${songName}, genre=${genre}`);
      const musicS3Url = await findFileInS3(shotstackMusicBucket, songName, genre, shotstackMusicSuffix);
      if (!musicS3Url) throw new Error(`Shotstack music file not found in S3 for song: ${songName}, genre: ${genre}`);

      const normalizedGenre = genre.replace(/\s/g, '');
      const shotstackTemplateId = shotstackTemplateMapping[normalizedGenre];
      if (!shotstackTemplateId) throw new Error(`Shotstack template ID not found for genre: ${genre}`);

      let mergeFields;
      if (bespokeBackgroundUrl) {
        console.log(`${logPrefix} Using bespoke background URL for Shotstack: ${bespokeBackgroundUrl}`);
        mergeFields = [
            { find: "hedra", replace: hedraVideoUrl }, { find: "name", replace: "" },
            { find: "altn", replace: "" }, { find: "happy", replace: "" }, // Clear text
            { find: "alth", replace: "" }, { find: "greetingbg", replace: bespokeBackgroundUrl }, // Use bespoke
            { find: "message", replace: "" }, { find: "sender", replace: "" }, // Clear text
            { find: "thumbnail_url", replace: initialThumbnailUrl || "" },
            { find: "music", replace: musicS3Url }, { find: "christmas", replace: "" },
            { find: "jinglemusic", replace: "0" },
        ];
      } else {
        console.log(`${logPrefix} Using standard/custom theme render URL for Shotstack: ${themeRenderUrl}`);
        mergeFields = [
            { find: "hedra", replace: hedraVideoUrl }, { find: "name", replace: "" },
            { find: "altn", replace: displayName || "" }, { find: "happy", replace: "" },
            { find: "alth", replace: "Happy Birthday" }, { find: "greetingbg", replace: themeRenderUrl },
            { find: "message", replace: message || "" }, { find: "sender", replace: yourName || "" },
            { find: "thumbnail_url", replace: initialThumbnailUrl || "" },
            { find: "music", replace: musicS3Url }, { find: "christmas", replace: "" },
            { find: "jinglemusic", replace: "0" },
        ];
      }

      const shotstackRequestBody = {
          id: shotstackTemplateId,
          merge: mergeFields,
      };
      const shotstackRenderResponse = await fetch(`https://api.shotstack.io/edit/v1/templates/render`, {
          method: 'POST',
          headers: { 'x-api-key': SHOTSTACK_API_KEY!, 'Content-Type': 'application/json', 'Accept': 'application/json' },
          body: JSON.stringify(shotstackRequestBody),
      });
      if (!shotstackRenderResponse.ok) throw new Error(`Shotstack render submission failed: ${await shotstackRenderResponse.text()}`);
      const shotstackRenderData = await shotstackRenderResponse.json() as ShotstackRenderResponse;
      if (!shotstackRenderData.success || !shotstackRenderData.response) {
          throw new Error(`Shotstack render failed to queue: ${shotstackRenderData.message || shotstackRenderData.response?.message}`);
      }
      const shotstackRenderId = shotstackRenderData.response.id;
      console.log(`${logPrefix} Submitted Shotstack render job: ${shotstackRenderId}`);

      // Update my_cards with Shotstack render ID (status is already COMPOSITING_FINAL_VIDEO)
      const { error: updateShotstackIdError } = await supabaseAdmin
        .from('my_cards')
        .update({ shotstack_render_id: shotstackRenderId }) // shotstack_status: 'queued' was already set implicitly by COMPOSITING_FINAL_VIDEO
        .eq('job_id', jobId);
      if (updateShotstackIdError) console.warn(`${logPrefix} Failed to update my_cards with Shotstack render ID: ${updateShotstackIdError.message}`);

      // The main status remains COMPOSITING_FINAL_VIDEO while polling Shotstack

      const pollingInterval = 20000;
      const pollingTimeout = 15 * 60 * 1000;
      const startTime = Date.now();
      let finalVideoUrl: string | undefined;

      while (Date.now() - startTime < pollingTimeout) {
          await new Promise(resolve => setTimeout(resolve, pollingInterval));
          const statusUrl = `https://api.shotstack.io/edit/v1/render/${shotstackRenderId}`;
          const statusResponse = await fetch(statusUrl, {
              headers: { 'x-api-key': SHOTSTACK_API_KEY!, 'Accept': 'application/json' },
          });
          if (!statusResponse.ok) {
              console.warn(`${logPrefix} Shotstack status poll failed with status ${statusResponse.status} for ${shotstackRenderId}`);
              continue;
          }
          const statusData = await statusResponse.json() as ShotstackStatusResponse;
          console.log(`${logPrefix} Polling Shotstack status for ${shotstackRenderId}:`, statusData.response?.status);

          if (statusData.success && statusData.response) {
              const currentStatus = statusData.response.status;
              if (currentStatus === 'done' && statusData.response.url) {
                  const rawVideoUrl = statusData.response.url;
                  const filename = path.basename(rawVideoUrl);
                  finalVideoUrl = `https://cdn.shotstack.io/au/v1/${SHOTSTACK_OWNER_ID}/${filename}`;
                  console.log(`${logPrefix} Shotstack render complete. Final CDN URL: ${finalVideoUrl}`);
                  break;
              } else if (currentStatus === 'failed') {
                  throw new Error(`Shotstack render failed: ${statusData.response.error}`);
              }
          } else {
              console.warn(`${logPrefix} Error polling Shotstack status or unexpected response:`, statusData.message);
          }
      }

      if (!finalVideoUrl) {
          const { error: timeoutError } = await supabaseAdmin
            .from('my_cards')
            .update({ status: 'FAILED', shotstack_status: 'timeout', error_message: 'Shotstack polling timed out.' })
            .eq('job_id', jobId);
          if (timeoutError) console.warn(`${logPrefix} Failed to update status to FAILED (timeout): ${timeoutError.message}`);
          throw new Error('Shotstack polling timed out.');
      }

      const { error: finalUpdateError } = await supabaseAdmin
          .from('my_cards')
          .update({ video_url: finalVideoUrl, status: 'COMPLETE', shotstack_status: 'complete' })
          .eq('job_id', jobId);
      if (finalUpdateError) throw new Error(`Error updating my_cards with final video URL: ${finalUpdateError.message}`);
      console.log(`${logPrefix} Successfully updated my_cards with final video URL and status COMPLETE for job_id ${jobId}`);

      // --- Create public share link and update my_cards ---
      let finalShortLinkeCardUrl = `${process.env.NEXT_PUBLIC_SITE_URL || 'http://localhost:3000'}`; // Default fallback

      if (job.data.myCardId && shotstackRenderId) { // shotstackRenderId should be available from earlier in the worker
        console.log(`${logPrefix} Attempting to insert into public_shared_cards for myCardId: ${job.data.myCardId}`);
        const { data: publicInsertData, error: publicInsertError } = await supabaseAdmin
            .from('public_shared_cards')
            .insert([{
                my_card_id: job.data.myCardId,
                thumbnail_url: initialThumbnailUrl, // Use the thumbnail passed in jobData
            }])
            .select('id') // Select the public ID
            .single();
