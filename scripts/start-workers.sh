#!/bin/sh

# Start the faceSwapWorker in the background
node dist-worker/workers/faceSwapWorker.js &

# Start the singingSelfieWorker in the background
node dist-worker/workers/singingSelfieWorker.js &

# Start the birthdaySlideshowWorker in the background
node dist-worker/workers/birthdaySlideshowWorker.js &
# Start the notificationWorker in the foreground
# This allows Docker logs to show output from this worker primarily,
# and the container will exit if this one crashes.
node dist-worker/workers/notificationWorker.js