"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.DatabaseFactory = void 0;
const ArangoAdapter_1 = __importDefault(require("../adapters/ArangoAdapter"));
const CassandraAdapter_1 = __importDefault(require("../adapters/CassandraAdapter"));
const CockroachDBAdapter_1 = __importDefault(require("../adapters/CockroachDBAdapter"));
const DynamoDBAdapter_1 = __importDefault(require("../adapters/DynamoDBAdapter"));
const ElasticsearchAdapter_1 = __importDefault(require("../adapters/ElasticsearchAdapter"));
const InfluxDBAdapter_1 = __importDefault(require("../adapters/InfluxDBAdapter"));
const MongoAdapter_1 = __importDefault(require("../adapters/MongoAdapter"));
const Neo4JAdapter_1 = __importDefault(require("../adapters/Neo4JAdapter"));
const OrientDBAdapter_1 = __importDefault(require("../adapters/OrientDBAdapter"));
const PostgressAdapter_1 = __importDefault(require("../adapters/PostgressAdapter"));
const RedisAdapter_1 = __importDefault(require("../adapters/RedisAdapter"));
const TimescaleAdapter_1 = __importDefault(require("../adapters/TimescaleAdapter"));
class DatabaseFactory {
    constructor() {
        this.adapters = {
            arango: ArangoAdapter_1.default,
            cassandra: CassandraAdapter_1.default,
            cockroachdb: CockroachDBAdapter_1.default,
            dynamodb: DynamoDBAdapter_1.default,
            elasticsearch: ElasticsearchAdapter_1.default,
            influxdb: InfluxDBAdapter_1.default,
            mongodb: MongoAdapter_1.default,
            neo4j: Neo4JAdapter_1.default,
            orientdb: OrientDBAdapter_1.default,
            postgres: PostgressAdapter_1.default,
            redis: RedisAdapter_1.default,
            timescale: TimescaleAdapter_1.default
        };
    }
    /**
     * Create database instance based on type
     * @param type - Database type
     * @param config - Connection configuration
     * @returns Database adapter instance
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
     * @returns List of supported database types
     */
    getSupportedDatabases() {
        return Object.keys(this.adapters);
    }
    /**
     * Check if database type is supported
     * @param type - Database type to check
     * @returns Whether the database type is supported
     */
    isSupported(type) {
        return type.toLowerCase() in this.adapters;
    }
    /**
     * Add custom adapter
     * @param type - Database type
     * @param AdapterClass - Adapter class implementation
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
     * @param type - Database type to remove
     */
    removeAdapter(type) {
        delete this.adapters[type.toLowerCase()];
    }
}
exports.DatabaseFactory = DatabaseFactory;
//# sourceMappingURL=DatabaseFactory.js.map