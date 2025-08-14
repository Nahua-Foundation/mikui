import { Topic, KafkaMessage, FavoriteMessage, KafkaCluster } from './types';

export const mockTopics: Topic[] = [
  { name: 'user-events', partitions: 3 },
  { name: 'order-events', partitions: 5 },
  { name: 'payment-events', partitions: 2 },
  { name: 'inventory-events', partitions: 4 },
];

export const mockMessages: KafkaMessage[] = [
  {
    partition: 0,
    key: 'user-123',
    offset: 1001,
    message: '{"userId": 123, "action": "login", "timestamp": "2025-08-14T10:30:00Z", "ip": "192.168.1.1"}',
    timestamp: '2025-08-14T10:30:00Z',
    headers: {
      'content-type': 'application/json',
      'source': 'user-service',
      'correlation-id': 'abc123',
      'event-version': '1.0'
    }
  },
  {
    partition: 1,
    key: 'user-456',
    offset: 1002,
    message: '{"userId": 456, "action": "purchase", "productId": "prod-789", "amount": 99.99, "currency": "USD"}',
    timestamp: '2025-08-14T10:31:15Z',
    headers: {
      'content-type': 'application/json',
      'source': 'order-service',
      'correlation-id': 'def456',
      'event-version': '2.1',
      'customer-tier': 'premium'
    }
  },
  {
    partition: 0,
    key: 'user-789',
    offset: 1003,
    message: '{"userId": 789, "action": "logout", "sessionDuration": 1800, "pagesViewed": 5}',
    timestamp: '2025-08-14T10:32:30Z',
    headers: {
      'content-type': 'application/json',
      'source': 'session-service',
      'correlation-id': 'ghi789',
      'event-version': '1.2',
      'session-type': 'web'
    }
  },
];

export const mockFavorites: FavoriteMessage[] = [
  {
    partition: 0,
    key: 'user-123',
    offset: 1001,
    message: '{"userId": 123, "action": "login", "timestamp": "2025-08-14T10:30:00Z", "ip": "192.168.1.1"}',
    timestamp: '2025-08-14T10:30:00Z',
    headers: {
      'content-type': 'application/json',
      'source': 'user-service',
      'correlation-id': 'abc123',
      'event-version': '1.0'
    },
    topicName: 'user-events',
    savedAt: '2025-08-14T09:15:00Z'
  },
  {
    partition: 1,
    key: 'user-456',
    offset: 1002,
    message: '{"userId": 456, "action": "purchase", "productId": "prod-789", "amount": 99.99, "currency": "USD"}',
    timestamp: '2025-08-14T10:31:15Z',
    headers: {
      'content-type': 'application/json',
      'source': 'order-service',
      'correlation-id': 'def456',
      'event-version': '2.1',
      'customer-tier': 'premium'
    },
    topicName: 'order-events',
    savedAt: '2025-08-14T08:45:00Z'
  }
];

export const mockClusters: KafkaCluster[] = [
  {
    id: '1',
    name: 'Development Cluster',
    brokers: 'localhost:9092',
    securityProtocol: 'PLAINTEXT',
    createdAt: '2025-08-10T14:30:00Z',
    lastUsed: '2025-08-14T10:15:00Z',
    isActive: true
  },
  {
    id: '2',
    name: 'Production Cluster',
    brokers: 'prod-kafka-1:9092,prod-kafka-2:9092,prod-kafka-3:9092',
    securityProtocol: 'SASL_SSL',
    saslMechanism: 'SCRAM-SHA-256',
    username: 'kafka-user',
    createdAt: '2025-08-12T09:00:00Z',
    lastUsed: '2025-08-13T16:45:00Z',
    isActive: false
  },
  {
    id: '3',
    name: 'Staging Environment',
    brokers: 'staging-kafka:9092',
    securityProtocol: 'SSL',
    keystorePath: '/etc/kafka/ssl/client.keystore.jks',
    truststorePath: '/etc/kafka/ssl/client.truststore.jks',
    createdAt: '2025-08-11T11:20:00Z',
    lastUsed: '2025-08-12T14:30:00Z',
    isActive: false
  }
];