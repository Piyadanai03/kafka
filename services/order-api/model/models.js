import { DataTypes, Model } from "sequelize";
import { sequelize } from "../db/connectDB.js";

export class Order extends Model {}
Order.init(
  {
    id: {
      type: DataTypes.UUID,
      primaryKey: true,
    },
    sku: {
      type: DataTypes.STRING(100),
      allowNull: false,
    },
    quantity: {
      type: DataTypes.INTEGER,
      allowNull: false,
    },
    customer_email: {
      type: DataTypes.STRING(255),
    },
  },
  {
    sequelize,
    modelName: "Order",
    tableName: "orders",
    timestamps: true,
    updatedAt: false,
    underscored: true,
  }
);

export class OutboxEvent extends Model {}
OutboxEvent.init(
  {
    id: {
      type: DataTypes.UUID,
      primaryKey: true,
    },
    topic: {
      type: DataTypes.STRING(255),
      allowNull: false,
    },
    message_key: {
      type: DataTypes.STRING(255),
    },
    payload: {
      type: DataTypes.JSONB,
      allowNull: false,
    },
  },
  {
    sequelize,
    modelName: "OutboxEvent",
    tableName: "outbox_events",
    timestamps: true, 
    updatedAt: false,
    underscored: true,
  }
);