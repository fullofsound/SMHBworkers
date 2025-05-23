# Use a lightweight Node.js image as the base
FROM node:22-alpine

# Install dos2unix for line ending conversion
RUN apk add --no-cache dos2unix

# Set the working directory in the container
WORKDIR /app

# Copy package.json and package-lock.json (or yarn.lock) to the working directory
# This allows us to install dependencies before copying the rest of the code
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the rest of the application code
COPY . .

# Convert script line endings from CRLF (Windows) to LF (Unix)
RUN dos2unix /app/scripts/start-workers.sh

# Compile the worker TypeScript code using the dedicated tsconfig
# Ensure tsconfig.worker.json exists and is configured correctly
RUN npx tsc --project tsconfig.worker.json

# Make the start-workers script executable
RUN chmod +x /app/scripts/start-workers.sh

# Command to run both workers using the script
CMD [ "/app/scripts/start-workers.sh" ]