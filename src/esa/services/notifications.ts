import type { ESAKVNamespace } from '../types';

const SIGNALR_RECORD_SEPARATOR = 0x1e;

interface WsSessionMeta {
  userId: string;
  deviceIdentifier: string | null;
  protocol: 'json' | 'messagepack';
  handshakeComplete: boolean;
}

export class ESANotificationsService {
  constructor(private redis: ESAKVNamespace) {}

  async addConnection(userId: string, deviceIdentifier: string | null): Promise<string> {
    const sessionId = `${userId}:${Date.now()}:${Math.random().toString(36).slice(2)}`;
    const meta: WsSessionMeta = {
      userId,
      deviceIdentifier,
      protocol: 'messagepack',
      handshakeComplete: false,
    };
    await this.redis.put(`ws:session:${sessionId}`, JSON.stringify(meta));
    await this.redis.put(`ws:user:${userId}:${sessionId}`, sessionId);
    if (deviceIdentifier) {
      await this.redis.put(`ws:device:${userId}:${deviceIdentifier}`, sessionId);
    }
    return sessionId;
  }

  async removeConnection(sessionId: string): Promise<WsSessionMeta | null> {
    const meta = await this.getSessionMeta(sessionId);
    if (!meta) return null;
    await this.redis.delete(`ws:session:${sessionId}`);
    await this.redis.delete(`ws:user:${meta.userId}:${sessionId}`);
    if (meta.deviceIdentifier) {
      await this.redis.delete(`ws:device:${meta.userId}:${meta.deviceIdentifier}`);
    }
    return meta;
  }

  async getSessionMeta(sessionId: string): Promise<WsSessionMeta | null> {
    const raw = await this.redis.get(`ws:session:${sessionId}`);
    return raw ? JSON.parse(raw) : null;
  }

  async updateSessionMeta(sessionId: string, update: Partial<WsSessionMeta>): Promise<void> {
    const meta = await this.getSessionMeta(sessionId);
    if (!meta) return;
    Object.assign(meta, update);
    await this.redis.put(`ws:session:${sessionId}`, JSON.stringify(meta));
  }

  async getOnlineDeviceIdentifiers(userId: string): Promise<string[]> {
    const prefix = `ws:device:${userId}:`;
    const result = await this.redis.list({ prefix });
    return result.keys.map(k => k.name.replace(prefix, ''));
  }
}