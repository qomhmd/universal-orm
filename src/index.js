const DatabaseFactory = require('./factory/DatabaseFactory');
const { ConfigValidator } = require('./utils/validators');
const { DatabaseError } = require('./utils/errors');

class UniversalORM {
    constructor() {
        this.connections = new Map();
        this.factory = new DatabaseFactory();
    }

    /**
     * Create a new database connection
     * @param {string} type - Database type (e.g., 'postgres', 'mongodb')
     * @param {Object} config - Connection configuration
     * @returns {Object} Database instance
     */
    connect(type, config) {
        try {
            // Validate configuration
            ConfigValidator.validate(type, config);

            // Check if connection already exists
            const connectionKey = `${type}-${config.database}`;
            if (this.connections.has(connectionKey)) {
                return this.connections.get(connectionKey);
            }

            // Create new connection
            const connection = this.factory.createDatabase(type, config);
            this.connections.set(connectionKey, connection);
            
            return connection;
        } catch (error) {
            throw new DatabaseError(`Connection failed: ${error.message}`);
        }
    }

    /**
     * Close all database connections
     */
    async closeAll() {
        const closePromises = [];
        for (const connection of this.connections.values()) {
            closePromises.push(connection.close());
        }
        await Promise.all(closePromises);
        this.connections.clear();
    }
}

// Create singleton instance
const db = new UniversalORM();

module.exports = db;
module.exports.UniversalORM = UniversalORM;
module.exports.BaseAdapter = BaseAdapter;
module.exports.adapters = require('./adapters');
