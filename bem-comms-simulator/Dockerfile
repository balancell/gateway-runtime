# Build stage
FROM node:14

# Create app directory
WORKDIR /usr/src/app

# Install app dependencies
# Copy package.json and package-lock.json to leverage Docker cache
COPY package*.json .

# Install npm dependencies
RUN npm install

# Copy the necessary app files (this excludes .git, node_modules, and other unnecessary files)
COPY package*.json ./
COPY app.js app.js

# Expose MQTT Port
EXPOSE 1883

# Start the app
CMD ["node", "app.js"]