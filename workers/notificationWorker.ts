// workers/notificationWorker.ts
import { Worker, Job } from 'bullmq';
import { connection } from '../src/lib/redis'; // Uses your existing Redis connection setup
import { supabaseAdmin } from '../src/lib/supabaseAdmin';
import { notificationProcessingQueue } from '../src/lib/queues';

console.log('Notification worker started, waiting for jobs on queue:', notificationProcessingQueue.name);

export interface NotificationJobData {
  userId: string;
  type: string; // e.g., 'faceswap_complete', 'card_ready'
  message: string;
  link?: string; // Optional link to the content
}

const worker = new Worker<NotificationJobData>(
  notificationProcessingQueue.name,
  async (job: Job<NotificationJobData>) => {
    const { userId, type, message, link } = job.data;
    console.log(`Processing notification job ${job.id} for user ${userId} with type ${type}`);

    if (!supabaseAdmin) {
      console.error(`Supabase admin client is not initialized. Cannot process notification job ${job.id}.`);
      throw new Error('Supabase admin client not initialized.');
    }

    try {
      const { data, error } = await supabaseAdmin
        .from('notifications')
        .insert([
          {
            user_id: userId,
            type: type,
            message: message,
            link: link,
            is_read: false, // Default to unread
          },
        ])
        .select(); // Optionally select to confirm insertion or get the ID

      if (error) {
        console.error(`Error inserting notification for job ${job.id} into Supabase:`, error.message);
        // Consider the type of error. Some errors might be temporary (network)
        // and BullMQ's default retry might handle them.
        // For other errors (e.g., constraint violations), retrying might not help.
        throw new Error(`Supabase insert error: ${error.message}`);
      }

      console.log(`Notification for job ${job.id} successfully inserted. Supabase response:`, data);

      // TODO: Implement Supabase Realtime event emission here if desired for instant client updates.
      // Example:
      // const { error: realtimeError } = await supabaseAdmin.channel(`notifications:${userId}`).send({
      //   type: 'broadcast',
      //   event: 'new_notification',
      //   payload: { message: 'You have a new notification!', notificationId: data?.[0]?.id },
      // });
      // if (realtimeError) {
      //   console.error(`Error sending realtime event for job ${job.id}:`, realtimeError);
      // }

    } catch (err: any) {
      console.error(`Failed to process notification job ${job.id}. Error: ${err.message}`);
      // Re-throw the error to let BullMQ handle retries/failures based on queue settings
      throw err;
    }
  },
  { 
    connection,
    // You can add more worker options here, like concurrency
    concurrency: parseInt(process.env.NOTIFICATION_WORKER_CONCURRENCY || "5"),
    removeOnComplete: { count: 1000, age: 24 * 3600 }, // Keep last 1000 completed jobs for 24 hours
    removeOnFail: { count: 5000, age: 7 * 24 * 3600 }      // Keep last 5000 failed jobs for 7 days
  }
);

worker.on('completed', (job: Job, result: any) => {
  console.log(`Notification job ${job.id} completed. Result:`, result);
});

worker.on('failed', (job: Job | undefined, err: Error) => {
  console.error(`Notification job ${job?.id || 'unknown'} failed. Error: ${err.message}`, err.stack);
});

worker.on('error', (err: Error) => {
  console.error('Notification worker encountered an error:', err);
});

console.log(`Notification worker listening for jobs on queue: ${notificationProcessingQueue.name}`);

// Graceful shutdown
const signals = ['SIGINT', 'SIGTERM', 'SIGQUIT'];
signals.forEach(signal => {
  process.on(signal, async () => {
    console.log(`Received ${signal}, shutting down notification worker...`);
    await worker.close();
    console.log('Notification worker closed.');
    process.exit(0);
  });
});