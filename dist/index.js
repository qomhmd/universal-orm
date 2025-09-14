"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.adapters = exports.BaseAdapter = exports.UniversalORM = void 0;
const DatabaseFactory_1 = require("./factory/DatabaseFactory");
const validators_1 = require("./utils/validators");
const errors_1 = require("./utils/errors");
const BaseAdapter_1 = require("./adapters/BaseAdapter");
Object.defineProperty(exports, "BaseAdapter", { enumerable: true, get: function () { return BaseAdapter_1.BaseAdapter; } });
class UniversalORM {
    constructor() {
        this.connections = new Map();
        this.factory = new DatabaseFactory_1.DatabaseFactory();
    }
    /**
     * Create a new database connection
     * @param type - Database type (e.g., 'postgres', 'mongodb')
     * @param config - Connection configuration
     * @returns Database instance
     */
    connect(type, config) {
        try {
            // Validate configuration
            validators_1.ConfigValidator.validate(type, config);
            // Check if connection already exists
            const connectionKey = `${type}-${config.database || config.uri || config.host}`;
            if (this.connections.has(connectionKey)) {
                return this.connections.get(connectionKey);
            }
            // Create new connection
            const connection = this.factory.createDatabase(type, config);
            this.connections.set(connectionKey, connection);
            return connection;
        }
        catch (error) {
            throw new errors_1.DatabaseError(`Connection failed: ${error.message}`);
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
    /**
     * Get all active connections
     */
    getConnections() {
        return this.connections;
    }
}
exports.UniversalORM = UniversalORM;
// Create singleton instance
const db = new UniversalORM();
exports.default = db;
exports.adapters = __importStar(require("./adapters"));
//# sourceMappingURL=index.js.map