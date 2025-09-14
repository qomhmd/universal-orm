import { DatabaseFactory } from './factory/DatabaseFactory';
import { ConfigValidator } from './utils/validators';
import { DatabaseError } from './utils/errors';
import { BaseAdapter } from './adapters/BaseAdapter';

export interface DatabaseConnection {
  [key: string]: any;
}

export class UniversalORM {
  private connections: Map<string, BaseAdapter>;
  private factory: DatabaseFactory;

  constructor() {
    this.connections = new Map();
    this.factory = new DatabaseFactory();
  }

  /**
   * Create a new database connection
   * @param type - Database type (e.g., 'postgres', 'mongodb')
   * @param config - Connection configuration
   * @returns Database instance
   */
  connect(type: string, config: Record<string, any>): BaseAdapter {
    try {
      // Validate configuration
      ConfigValidator.validate(type, config);

      // Check if connection already exists
      const connectionKey = `${type}-${config.database || config.uri || config.host}`;
      if (this.connections.has(connectionKey)) {
        return this.connections.get(connectionKey)!;
      }

      // Create new connection
      const connection = this.factory.createDatabase(type, config);
      this.connections.set(connectionKey, connection);

      return connection;
    } catch (error) {
      throw new DatabaseError(`Connection failed: ${(error as Error).message}`);
    }
  }

  /**
   * Close all database connections
   */
  async closeAll(): Promise<void> {
    const closePromises: Promise<void>[] = [];
    for (const connection of this.connections.values()) {
      closePromises.push(connection.close());
    }
    await Promise.all(closePromises);
    this.connections.clear();
  }

  /**
   * Get all active connections
   */
  getConnections(): Map<string, BaseAdapter> {
    return this.connections;
  }
}

// Create singleton instance
const db = new UniversalORM();

export default db;
export { BaseAdapter };
export * as adapters from './adapters';