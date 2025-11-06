import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'seed-producer',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();

// รายการ SKU 10 อย่างสำหรับสุ่ม
const skus = [
  'SKU-TSHIRT-RED', 'SKU-TSHIRT-BLK', 'SKU-MUG-LOGO', 'SKU-STICKER-SET',
  'SKU-HOODIE-BLK', 'SKU-HOODIE-WHT', 'SKU-CAP-BLK', 'SKU-BEANIE',
  'SKU-SOCKS-PAIR', 'SKU-POSTER-LARGE'
];

const getRandomSku = () => skus[Math.floor(Math.random() * skus.length)];

const run = async () => {
  await producer.connect();
  console.log('Producer connected...');

  console.log('Sending 100 random orders...');

  for (let i = 1; i <= 100; i++) {
    const orderId = `ORD-2025-${i.toString().padStart(4, '0')}`;
    const sku = getRandomSku();
    
    const order = {
      orderId: orderId,
      sku: sku,
      quantity: Math.ceil(Math.random() * 3), // สุ่มจำนวน 1-3
      timestamp: new Date().toISOString()
    };

    await producer.send({
      topic: 'orders.created',
      messages: [
        {
          // นี่คือ Key สำคัญ! เราใช้ SKU เป็น Key
          key: sku, 
          value: JSON.stringify(order) 
        }
      ]
    });

    if (i % 10 === 0) console.log(`Sent ${i} orders...`);
  }

  console.log('All 100 orders sent!');
  await producer.disconnect();
};

run().catch(e => console.error('[Producer] Error:', e));