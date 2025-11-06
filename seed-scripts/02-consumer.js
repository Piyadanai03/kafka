import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'hello-consumer',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ 
  groupId: 'order-checkers' // <--- Consumer Group ID สำคัญมาก
});

const run = async () => {
  await consumer.connect();
  console.log('Consumer connected...');

  // Subscribe Topic
  await consumer.subscribe({ 
    topic: 'orders.created',
    fromBeginning: true // อ่านตั้งแต่ต้น
  });

  // เริ่มรัน Consumer
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      // นี่คือส่วนที่โจทย์ต้องการ!
      console.log(`[CONSUMER] Received:
        Topic: ${topic}
        Partition: ${partition}
        Offset: ${message.offset}
        Key: ${message.key?.toString()}
        Value: ${message.value.toString()}
      -------------------`);
    },
  });
};

run().catch(e => console.error('[Consumer] Error:', e));