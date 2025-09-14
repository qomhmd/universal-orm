const ArangoAdapter = require('../adapters/ArangoAdapter');
const CassandraAdapter = require('../adapters/CassandraAdapter');
const CockroachDBAdapter = require('../adapters/CockroachDBAdapter');
const DynamoDBAdapter = require('../adapters/DynamoDBAdapter');
const ElasticsearchAdapter = require('../adapters/ElasticsearchAdapter');
const InfluxDBAdapter = require('../adapters/InfluxDBAdapter');
const MongoAdapter = require('../adapters/MongoAdapter');
const Neo4jAdapter = require('../adapters/Neo4JAdapter');
const OrientDBAdapter = require('../adapters/OrientDBAdapter');
const PostgresAdapter = require('../adapters/PostgresAdapter');
const RedisAdapter = require('../adapters/RedisAdapter');
const TimescaleAdapter = require('../adapters/TimescaleAdapter');

class DatabaseFactory {
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
     * @param {string} type - Database type
     * @param {Object} config - Connection configuration
     * @returns {Object} Database adapter instance
     */
    createDatabase(type, config) {
        const AdapterClass = this.adapters[type.toLowerCase()];
        if (!AdapterClass) {
            throw new Error(`Unsupported database type: ${type}`);
        }
        return new AdapterClass(config);
    }

    /**
     * Get list of supported databases
     * @returns {Array} List of supported database types
     */
    getSupportedDatabases() {
        return Object.keys(this.adapters);
    }

    /**
     * Check if database type is supported
     * @param {string} type - Database type to check
     * @returns {boolean} Whether the database type is supported
     */
    isSupported(type) {
        return type.toLowerCase() in this.adapters;
    }

    /**
     * Add custom adapter
     * @param {string} type - Database type
     * @param {Class} AdapterClass - Adapter class implementation
     */
    addAdapter(type, AdapterClass) {
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
     * @param {string} type - Database type to remove
     */
    removeAdapter(type) {
        delete this.adapters[type.toLowerCase()];
    }
}

module.exports = DatabaseFactory;