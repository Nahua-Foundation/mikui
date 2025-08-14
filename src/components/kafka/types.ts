export interface KafkaMessage {
  partition: number;
  key: string;
  offset: number;
  message: string;
  timestamp: string;
  headers: Record<string, string>;
}

export interface FavoriteMessage extends KafkaMessage {
  topicName: string;
  savedAt: string;
}

export interface Topic {
  name: string;
  partitions: number;
}

export interface MessageFilters {
  key: string;
  message: string;
}

export interface KafkaCluster {
  id: string;
  name: string;
  brokers: string;
  securityProtocol: string;
  saslMechanism?: string;
  username?: string;
  password?: string;
  keystorePath?: string;
  keystorePassword?: string;
  truststorePath?: string;
  truststorePassword?: string;
  createdAt: string;
  lastUsed?: string;
  isActive?: boolean;
}