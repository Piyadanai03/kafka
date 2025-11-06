import express from 'express';
import { Kafka } from 'kafkajs';
import { randomUUID } from 'crypto'; // สำหรับสร้าง orderId

// --- Kafka Producer Setup ---
const kafka = new Kafka({
  clientId: 'order-api',
  brokers: ['localhost:9092'],
});
const producer = kafka.producer();

// --- Express App Setup ---
const app = express();
app.use(express.json());

// --- Endpoint: POST /orders ---
app.post('/orders', async (req, res) => {
  const { sku, quantity } = req.body;
  if (!sku || !quantity) {
    return res.status(400).json({ message: 'SKU and quantity are required.' });
  }

  const orderId = randomUUID(); // สร้าง ID ออเดอร์
  const order = {
    orderId: orderId,
    sku: sku,
    quantity: parseInt(quantity, 10),
    timestamp: new Date().toISOString()
  };

  try {
    // ส่ง event ไปที่ "orders.created"
    await producer.send({
      topic: 'orders.created',
      messages: [
        {
          key: orderId, // <-- ใช้ orderId เป็น Key ตามโจทย์
          value: JSON.stringify(order)
        }
      ]
    });

    console.log(`[Order API] Order produced: ${orderId}`);
    res.status(202).json({ 
      message: 'Order received, processing...',
      orderId: orderId 
    });

  } catch (error) {
    console.error('[Order API] Error producing order:', error);
    res.status(500).json({ message: 'Error processing order.' });
  }
});

// --- Run Server ---
const run = async () => {
  await producer.connect();
  app.listen(3000, () => {
    console.log('Order API listening on port 3000');
  });
};

run().catch(console.error);