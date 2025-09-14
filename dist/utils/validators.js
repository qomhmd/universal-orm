"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ConfigValidator = void 0;
class ConfigValidator {
    static validate(type, config) {
        if (!type || typeof type !== 'string') {
            throw new Error('Database type must be a non-empty string');
        }
        if (!config || typeof config !== 'object') {
            throw new Error('Configuration must be an object');
        }
        // Basic configuration requirements
        const requiredFields = {
            postgres: ['host', 'port', 'database', 'user', 'password'],
            mongodb: ['uri', 'database'],
            redis: ['host', 'port'],
            cassandra: ['contactPoints', 'localDataCenter', 'keyspace'],
            dynamodb: ['region'],
            elasticsearch: ['node'],
            neo4j: ['uri', 'user', 'password'],
            influxdb: ['url', 'token', 'org', 'bucket'],
            arango: ['url', 'database', 'username', 'password'],
            orientdb: ['host', 'port', 'username', 'password', 'database'],
            timescale: ['host', 'port', 'database', 'user', 'password'],
            cockroachdb: ['host', 'port', 'database', 'user', 'password']
        };
        const fields = requiredFields[type.toLowerCase()];
        if (fields) {
            for (const field of fields) {
                if (!config[field]) {
                    throw new Error(`Missing required field: ${field}`);
                }
            }
        }
    }
}
exports.ConfigValidator = ConfigValidator;
//# sourceMappingURL=validators.js.map