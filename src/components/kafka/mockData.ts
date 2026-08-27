import { KafkaCluster } from './types';

// Прежние mockTopics / mockMessages / mockFavorites удалены: топики и сообщения
// теперь приезжают из Kafka, а показывать пользователю выдуманные сообщения как
// настоящие — плохая идея.
//
// Здесь остался только стартовый список кластеров. Он никуда не сохраняется:
// персистентность (конфиги в app_config_dir, пароли в keychain) — Фаза 3.
// Один локальный кластер как пример формы, без выдуманных продовых адресов.
export const mockClusters: KafkaCluster[] = [
  {
    id: 'local',
    name: 'Local',
    brokers: 'localhost:9092',
    securityProtocol: 'PLAINTEXT',
    createdAt: new Date().toISOString(),
    isActive: false,
  },
];
