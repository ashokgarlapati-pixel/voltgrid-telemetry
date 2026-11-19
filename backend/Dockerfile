FROM node:20-bookworm-slim

# Install system dependencies required for native module compilation (better-sqlite3)
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    make \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Ensure /data directory exists for persistent SQLite database volume
RUN mkdir -p /data

# Copy package descriptors first to leverage Docker layer caching
COPY package*.json ./

# Install production dependencies
RUN npm install --omit=dev

# Copy application source code and migrations
COPY . .

# Expose API port
EXPOSE 3000

# Default environment configuration
ENV PORT=3000
ENV DB_PATH=/data/voltgrid.db
ENV NODE_ENV=production

# Start the telemetry backend server
CMD ["node", "src/server.js"]
