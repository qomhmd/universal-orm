import { BaseAdapter } from '../adapters/BaseAdapter';
import ArangoAdapter from '../adapters/ArangoAdapter';
import CassandraAdapter from '../adapters/CassandraAdapter';
import CockroachDBAdapter from '../adapters/CockroachDBAdapter';
import DynamoDBAdapter from '../adapters/DynamoDBAdapter';
import ElasticsearchAdapter from '../adapters/ElasticsearchAdapter';
import InfluxDBAdapter from '../adapters/InfluxDBAdapter';
import MongoAdapter from '../adapters/MongoAdapter';
import Neo4jAdapter from '../adapters/Neo4JAdapter';
import OrientDBAdapter from '../adapters/OrientDBAdapter';
import PostgresAdapter from '../adapters/PostgressAdapter';
import RedisAdapter from '../adapters/RedisAdapter';
import TimescaleAdapter from '../adapters/TimescaleAdapter';

export class DatabaseFactory {
  private adapters: Record<string, new (config: Record<string, any>) => BaseAdapter>;

  constructor() {
    this.adapters = {
      arango: ArangoAdapter,
      cassandra: CassandraAdapter,
      cockroachdb: CockroachDBAdapter,
      dynamodb: DynamoDBAdapter,
      elasticsearch: ElasticsearchAdapter,
      influxdb: InfluxDBAdapter,
      mongodb: MongoAdapter,
      neo4j: Neo4jAdapter,
      orientdb: OrientDBAdapter,
      postgres: PostgresAdapter,
      redis: RedisAdapter,
      timescale: TimescaleAdapter
    };
  }

  /**
   * Create database instance based on type
   * @param type - Database type
   * @param config - Connection configuration
   * @returns Database adapter instance
   */
  createDatabase(type: string, config: Record<string, any>): BaseAdapter {
    const AdapterClass = this.adapters[type.toLowerCase()];
    if (!AdapterClass) {
      throw new Error(`Unsupported database type: ${type}`);
    }
    return new AdapterClass(config);
  }

  /**
   * Get list of supported databases
   * @returns List of supported database types
   */
  getSupportedDatabases(): string[] {
    return Object.keys(this.adapters);
  }

  /**
   * Check if database type is supported
   * @param type - Database type to check
   * @returns Whether the database type is supported
   */
  isSupported(type: string): boolean {
    return type.toLowerCase() in this.adapters;
  }

  /**
   * Add custom adapter
   * @param type - Database type
   * @param AdapterClass - Adapter class implementation
   */
  addAdapter(type: string, AdapterClass: new (config: Record<string, any>) => BaseAdapter): void {
    if (typeof type !== 'string' || !type) {
      throw new Error('Database type must be a non-empty string');
    }
    if (typeof AdapterClass !== 'function') {
      throw new Error('Adapter must be a class');
    }
    this.adapters[type.toLowerCase()] = AdapterClass;
  }

  /**
   * Remove adapter
   * @param type - Database type to remove
   */
  removeAdapter(type: string): void {
    delete this.adapters[type.toLowerCase()];
  }
}