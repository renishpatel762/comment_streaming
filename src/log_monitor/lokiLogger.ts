// lokiLogger.ts
import winston from 'winston';
import LokiTransport from 'winston-loki';

const serviceName = process.env.SERVICE_NAME || 'comment-streaming';
const environment = process.env.NODE_ENV || 'development';
const lokiUrl = process.env.LOKI_URL || 'http://loki:3100';

// Create Winston logger with Loki transport
export const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  defaultMeta: {
    service: serviceName,
    environment: environment,
    version: process.env.SERVICE_VERSION || '1.0.0'
  },
  transports: [
    // Console transport for development
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.simple()
      )
    }),
    // Loki transport for centralized logging
    new LokiTransport({
      host: lokiUrl,
      labels: {
        service: serviceName,
        environment: environment,
        job: 'comment-streaming'
      },
      json: true,
      format: winston.format.json(),
      replaceTimestamp: true,
      onConnectionError: (err) => {
        console.error('Loki connection error:', err);
      }
    })
  ],
  // Handle uncaught exceptions and rejections
  exceptionHandlers: [
    new winston.transports.Console(),
    new LokiTransport({
      host: lokiUrl,
      labels: {
        service: serviceName,
        environment: environment,
        level: 'error',
        type: 'exception'
      }
    })
  ],
  rejectionHandlers: [
    new winston.transports.Console(),
    new LokiTransport({
      host: lokiUrl,
      labels: {
        service: serviceName,
        environment: environment,
        level: 'error',
        type: 'rejection'
      }
    })
  ]
});