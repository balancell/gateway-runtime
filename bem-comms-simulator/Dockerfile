# Stage 1: Build stage
FROM node:20 AS build

# Create app directory
WORKDIR /usr/src/app

# Install app dependencies
# Copy package.json and package-lock.json to leverage Docker cache(faster rebuild)
COPY package*.json package-lock.json ./

# Install npm dependencies
RUN npm install

# Stage 2: Runtime Stage: Use a slim version of node
FROM node:20-slim AS runtime

# Switch to app directory
WORKDIR /usr/src/app

# Copy node dependencies from the build stage
COPY --from=build /usr/src/app/node_modules/ /usr/src/app/node_modules/

# Copy the necessary app files (this excludes .git, node_modules, and other unnecessary files)
COPY package*.json ./
COPY app.js app.js

# Expose MQTT Port
EXPOSE 1883

# Start the app command
CMD ["node", "app.js"]