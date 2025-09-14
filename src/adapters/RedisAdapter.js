
const Redis = require('ioredis')
const BaseAdapter = require('./BaseAdapter');
const { DatabaseError } = require('../utils/errors');

class RedisAdapter extends BaseAdapter {
    constructor(config) {
        super();
        this.config = config;
        this.client = null;
        this.subscriber = null;
        this.publisher = null;
    }

    /**
     * Initialize Redis connections
     */
    async initialize() {
        try {
            // Main client for regular operations
            this.client = new Redis({
                host: this.config.host,
                port: this.config.port,
                password: this.config.password,
                db: this.config.db || 0,
                retryStrategy: (times) => {
                    const delay = Math.min(times * 50, 2000);
                    return delay;
                }
            });

            // Separate clients for pub/sub to avoid blocking
            if (this.config.enablePubSub) {
                this.subscriber = new Redis(this.config);
                this.publisher = new Redis(this.config);
            }

            await this._ping();
            return this.client;
        } catch (error) {
            throw new DatabaseError(`Redis initialization failed: ${error.message}`);
        }
    }

    /**
     * Key-Value Operations
     */
    async create(key, value, options = {}) {
        try {
            const { ttl, nx, xx } = options;
            let args = [key, this._serialize(value)];
            
            if (ttl) {
                args.push('EX', ttl);
            }
            if (nx) {
                args.push('NX');
            }
            if (xx) {
                args.push('XX');
            }

            return await this.client.set(...args);
        } catch (error) {
            throw new DatabaseError(`Redis SET operation failed: ${error.message}`);
        }
    }

    async get(key) {
        try {
            const value = await this.client.get(key);
            return this._deserialize(value);
        } catch (error) {
            throw new DatabaseError(`Redis GET operation failed: ${error.message}`);
        }
    }

    async delete(key) {
        try {
            return await this.client.del(key);
        } catch (error) {
            throw new DatabaseError(`Redis DELETE operation failed: ${error.message}`);
        }
    }

    /**
     * Hash Operations
     */
    async hset(key, field, value) {
        try {
            return await this.client.hset(key, field, this._serialize(value));
        } catch (error) {
            throw new DatabaseError(`Redis HSET operation failed: ${error.message}`);
        }
    }

    async hget(key, field) {
        try {
            const value = await this.client.hget(key, field);
            return this._deserialize(value);
        } catch (error) {
            throw new DatabaseError(`Redis HGET operation failed: ${error.message}`);
        }
    }

    async hgetall(key) {
        try {
            const hash = await this.client.hgetall(key);
            return Object.entries(hash).reduce((acc, [field, value]) => {
                acc[field] = this._deserialize(value);
                return acc;
            }, {});
        } catch (error) {
            throw new DatabaseError(`Redis HGETALL operation failed: ${error.message}`);
        }
    }

    /**
     * List Operations
     */
    async lpush(key, ...values) {
        try {
            return await this.client.lpush(key, ...values.map(this._serialize));
        } catch (error) {
            throw new DatabaseError(`Redis LPUSH operation failed: ${error.message}`);
        }
    }

    async rpush(key, ...values) {
        try {
            return await this.client.rpush(key, ...values.map(this._serialize));
        } catch (error) {
            throw new DatabaseError(`Redis RPUSH operation failed: ${error.message}`);
        }
    }

    async lrange(key, start, stop) {
        try {
            const values = await this.client.lrange(key, start, stop);
            return values.map(this._deserialize);
        } catch (error) {
            throw new DatabaseError(`Redis LRANGE operation failed: ${error.message}`);
        }
    }

    /**
     * Set Operations
     */
    async sadd(key, ...members) {
        try {
            return await this.client.sadd(key, ...members.map(this._serialize));
        } catch (error) {
            throw new DatabaseError(`Redis SADD operation failed: ${error.message}`);
        }
    }

    async smembers(key) {
        try {
            const members = await this.client.smembers(key);
            return members.map(this._deserialize);
        } catch (error) {
            throw new DatabaseError(`Redis SMEMBERS operation failed: ${error.message}`);
        }
    }

    /**
     * Sorted Set Operations
     */
    async zadd(key, score, member) {
        try {
            return await this.client.zadd(key, score, this._serialize(member));
        } catch (error) {
            throw new DatabaseError(`Redis ZADD operation failed: ${error.message}`);
        }
    }

    async zrange(key, start, stop, withScores = false) {
        try {
            const args = [key, start, stop];
            if (withScores) args.push('WITHSCORES');
            
            const results = await this.client.zrange(...args);
            
            if (!withScores) {
                return results.map(this._deserialize);
            }

            // Parse results with scores
            const parsed = [];
            for (let i = 0; i < results.length; i += 2) {
                parsed.push({
                    member: this._deserialize(results[i]),
                    score: parseFloat(results[i + 1])
                });
            }
            return parsed;
        } catch (error) {
            throw new DatabaseError(`Redis ZRANGE operation failed: ${error.message}`);
        }
    }

    /**
     * Pub/Sub Operations
     */
    async publish(channel, message) {
        try {
            if (!this.publisher) {
                throw new Error('Pub/Sub not enabled');
            }
            return await this.publisher.publish(channel, this._serialize(message));
        } catch (error) {
            throw new DatabaseError(`Redis PUBLISH operation failed: ${error.message}`);
        }
    }

    async subscribe(channel, callback) {
        try {
            if (!this.subscriber) {
                throw new Error('Pub/Sub not enabled');
            }
            await this.subscriber.subscribe(channel);
            this.subscriber.on('message', (ch, message) => {
                if (ch === channel) {
                    callback(this._deserialize(message));
                }
            });
        } catch (error) {
            throw new DatabaseError(`Redis SUBSCRIBE operation failed: ${error.message}`);
        }
    }

    /**
     * Transaction Operations
     */
    async transaction(callback) {
        const multi = this.client.multi();
        try {
            await callback(multi);
            return await multi.exec();
        } catch (error) {
            throw new DatabaseError(`Redis transaction failed: ${error.message}`);
        }
    }

    /**
     * Cache Operations
     */
    async cache(key, callback, ttl = 3600) {
        try {
            let value = await this.get(key);
            if (value === null) {
                value = await callback();
                await this.set(key, value, { ttl });
            }
            return value;
        } catch (error) {
            throw new DatabaseError(`Cache operation failed: ${error.message}`);
        }
    }

    /**
     * Close connections
     */
    async close() {
        try {
            await this.client.quit();
            if (this.subscriber) await this.subscriber.quit();
            if (this.publisher) await this.publisher.quit();
        } catch (error) {
            throw new DatabaseError(`Redis close failed: ${error.message}`);
        }
    }

    // Private helper methods
    async _ping() {
        try {
            await this.client.ping();
        } catch (error) {
            throw new Error('Redis connection failed');
        }
    }

    _serialize(value) {
        return typeof value === 'string' ? value : JSON.stringify(value);
    }

    _deserialize(value) {
        if (value === null) return null;
        try {
            return JSON.parse(value);
        } catch {
            return value;
        }
    }
}

module.exports = RedisAdapter;