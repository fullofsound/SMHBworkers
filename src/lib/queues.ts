import { Queue } from "bullmq";
import { connection } from "./redis";

export const faceSwapQueue = new Queue("face-swap", { connection });

export const notificationProcessingQueue = new Queue("notification-processing", { connection });

export const singingSelfieQueue = new Queue("singing-selfie-processing", { connection });

export const birthdaySlideshowQueue = new Queue("birthday-slideshow-processing", { connection });

export const webhookProcessingQueue = new Queue("webhook-processing", { connection });