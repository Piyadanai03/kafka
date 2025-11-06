import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'admin-client',
  brokers: ['localhost:9092'],
});

const admin = kafka.admin();

const run = async () => {
  await admin.connect();
  console.log('Admin connected...');

  await admin.createTopics({
    topics: [
      {
        topic: 'inventory.stock',
        numPartitions: 1, 
        replicationFactor: 1,
        configEntries: [
          { name: 'cleanup.policy', value: 'compact' }, // <--- สำคัญ
          { name: 'min.cleanable.dirty.ratio', value: '0.01' },
          { name: 'segment.ms', value: '1000' }, // บังคับให้ compact บ่อยๆ (สำหรับ dev)
        ],
      },
      { topic: 'orders.validated', numPartitions: 3, replicationFactor: 1 },
      { topic: 'orders.rejected', numPartitions: 3, replicationFactor: 1 },
      { topic: 'inventory.dlq', numPartitions: 1, replicationFactor: 1 }, // DLQ
    ],
  });

  console.log('App topics created!');
  await admin.disconnect();
};

run().catch(console.error);