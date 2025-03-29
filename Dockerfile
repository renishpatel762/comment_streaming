# Use official Node.js image as base
FROM node:22

# Set the working directory inside the container
WORKDIR /app

# Copy package.json and package-lock.json first (for caching)
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the rest of the application files
COPY . .

# Build TypeScript files
RUN npm run build

# Expose the application port (change based on your app)
EXPOSE ${PORT}

# Set the command to run the app
CMD ["npm", "start"]
