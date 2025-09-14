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
const mongoose_1 = __importStar(require("mongoose"));
const BaseAdapter_1 = require("./BaseAdapter");
const errors_1 = require("../utils/errors");
class MongoAdapter extends BaseAdapter_1.BaseAdapter {
    constructor(config) {
        super(config);
        this.models = new Map();
        this.connection = null;
    }
    /**
     * Initialize MongoDB connection
     */
    async initialize(options) {
        try {
            this.connection = await mongoose_1.default.createConnection(this.config.uri);
            // Connection error handling
            this.connection.on('error', (error) => {
                throw new errors_1.DatabaseError(`MongoDB connection error: ${error.message}`);
            });
        }
        catch (error) {
            throw new errors_1.DatabaseError(`MongoDB initialization failed: ${error.message}`);
        }
    }
    /**
     * Create a new record in the database
     */
    async create(model, data, options) {
        try {
            const modelInstance = this._getModel(model);
            const result = await modelInstance.create(data, options);
            if (Array.isArray(result)) {
                return result.map((doc) => doc.toObject());
            }
            return result.toObject();
        }
        catch (error) {
            throw new errors_1.DatabaseError(`Create operation failed: ${error.message}`);
        }
    }
    /**
     * Find a single record
     */
    async findOne(model, query, options) {
        try {
            const modelInstance = this._getModel(model);
            const { select, populate } = options || {};
            let mongoQuery = modelInstance.findOne(this._convertQuery(query));
            if (select)
                mongoQuery = mongoQuery.select(select);
            if (populate)
                mongoQuery = mongoQuery.populate(populate);
            const result = await mongoQuery.exec();
            return result ? result.toObject() : null;
        }
        catch (error) {
            throw new errors_1.DatabaseError(`Find operation failed: ${error.message}`);
        }
    }
    /**
     * Find multiple records
     */
    async findMany(model, query = {}, options) {
        try {
            const modelInstance = this._getModel(model);
            const { select, populate, limit, offset, sort } = options || {};
            let mongoQuery = modelInstance.find(this._convertQuery(query));
            if (select)
                mongoQuery = mongoQuery.select(select);
            if (populate)
                mongoQuery = mongoQuery.populate(populate);
            if (limit)
                mongoQuery = mongoQuery.limit(limit);
            if (offset)
                mongoQuery = mongoQuery.skip(offset);
            if (sort)
                mongoQuery = mongoQuery.sort(sort);
            const results = await mongoQuery.exec();
            return results.map((doc) => doc.toObject());
        }
        catch (error) {
            throw new errors_1.DatabaseError(`Find operation failed: ${error.message}`);
        }
    }
    /**
     * Update records
     */
    async update(model, query, data, options) {
        try {
            const modelInstance = this._getModel(model);
            const result = await modelInstance.updateMany(this._convertQuery(query), this._convertUpdateOperations(data), options);
            return result;
        }
        catch (error) {
            throw new errors_1.DatabaseError(`Update operation failed: ${error.message}`);
        }
    }
    /**
     * Delete records
     */
    async delete(model, query, options) {
        try {
            const modelInstance = this._getModel(model);
            return await modelInstance.deleteMany(this._convertQuery(query));
        }
        catch (error) {
            throw new errors_1.DatabaseError(`Delete operation failed: ${error.message}`);
        }
    }
    /**
     * Execute operations in a transaction
     */
    async transaction(callback, options) {
        if (!this.connection) {
            throw new errors_1.DatabaseError('Database connection not initialized');
        }
        const session = await this.connection.startSession();
        try {
            session.startTransaction();
            const result = await callback();
            await session.commitTransaction();
            return result;
        }
        catch (error) {
            await session.abortTransaction();
            throw new errors_1.DatabaseError(`Transaction failed: ${error.message}`);
        }
        finally {
            session.endSession();
        }
    }
    /**
     * Close database connection
     */
    async close() {
        if (this.connection) {
            await this.connection.close();
            this.connection = null;
        }
    }
    /**
     * Create a new model/collection/table
     */
    async createModel(model, schema, options) {
        try {
            if (!this.connection) {
                throw new Error('Database connection not initialized');
            }
            const mongooseSchema = new mongoose_1.Schema(this._convertSchema(schema), {
                timestamps: true,
                strict: true,
                ...options
            });
            // Add indexes
            this._addSchemaIndexes(mongooseSchema, schema);
            const modelInstance = this.connection.model(model, mongooseSchema);
            this.models.set(model, modelInstance);
        }
        catch (error) {
            throw new errors_1.DatabaseError(`Failed to create model: ${error.message}`);
        }
    }
    /**
     * Drop a model/collection/table
     */
    async dropModel(model, options) {
        try {
            if (!this.connection) {
                throw new Error('Database connection not initialized');
            }
            const conn = this.connection;
            if (!conn.db) {
                throw new Error('Database not available');
            }
            await conn.db.dropCollection(model);
            this.models.delete(model);
        }
        catch (error) {
            throw new errors_1.DatabaseError(`Failed to drop model: ${error.message}`);
        }
    }
    /**
     * Count records
     */
    async count(model, query = {}, options) {
        try {
            const modelInstance = this._getModel(model);
            return await modelInstance.countDocuments(this._convertQuery(query));
        }
        catch (error) {
            throw new errors_1.DatabaseError(`Count operation failed: ${error.message}`);
        }
    }
    /**
     * Check if records exist
     */
    async exists(model, query, options) {
        try {
            const count = await this.count(model, query, options);
            return count > 0;
        }
        catch (error) {
            throw new errors_1.DatabaseError(`Exists operation failed: ${error.message}`);
        }
    }
    /**
     * Bulk insert records
     */
    async bulkCreate(model, data, options) {
        try {
            const modelInstance = this._getModel(model);
            const result = await modelInstance.insertMany(data, options || {});
            return { insertedCount: result.length, insertedIds: result.map((doc) => doc._id) };
        }
        catch (error) {
            throw new errors_1.DatabaseError(`Bulk create operation failed: ${error.message}`);
        }
    }
    /**
     * Bulk update records
     */
    async bulkUpdate(model, updates, options) {
        try {
            const modelInstance = this._getModel(model);
            const bulkOps = updates.map(update => ({
                updateMany: {
                    filter: this._convertQuery(update.query),
                    update: this._convertUpdateOperations(update.update),
                    ...update.options
                }
            }));
            const result = await modelInstance.bulkWrite(bulkOps, options);
            return result;
        }
        catch (error) {
            throw new errors_1.DatabaseError(`Bulk update operation failed: ${error.message}`);
        }
    }
    /**
     * Perform aggregation
     */
    async aggregate(model, pipeline, options) {
        try {
            const modelInstance = this._getModel(model);
            const result = await modelInstance.aggregate(Array.isArray(pipeline) ? pipeline : [pipeline]);
            return result;
        }
        catch (error) {
            throw new errors_1.DatabaseError(`Aggregation operation failed: ${error.message}`);
        }
    }
    /**
     * Execute raw query
     */
    async query(query, params, options) {
        try {
            if (!this.connection) {
                throw new Error('Database connection not initialized');
            }
            const conn = this.connection;
            if (typeof query === 'string') {
                // Raw MongoDB command
                if (!conn.db) {
                    throw new Error('Database not available');
                }
                return await conn.db.command(JSON.parse(query));
            }
            else {
                // Direct collection operation
                if (!conn.db) {
                    throw new Error('Database not available');
                }
                const collection = conn.db.collection(query.collection);
                const operation = query.operation;
                const method = collection[operation];
                if (typeof method === 'function') {
                    const args = Array.isArray(params) ? params : [params];
                    return await method.apply(collection, args);
                }
                throw new Error(`Unknown operation: ${operation}`);
            }
        }
        catch (error) {
            throw new errors_1.DatabaseError(`Query operation failed: ${error.message}`);
        }
    }
    // Private helper methods
    _getModel(modelName) {
        const model = this.models.get(modelName);
        if (!model) {
            throw new Error(`Model ${modelName} not found`);
        }
        return model;
    }
    _convertSchema(schema) {
        const mongooseSchema = {};
        for (const [field, definition] of Object.entries(schema)) {
            mongooseSchema[field] = this._convertFieldType(definition);
        }
        return mongooseSchema;
    }
    _convertFieldType(definition) {
        const typeMap = {
            'string': String,
            'number': Number,
            'integer': Number,
            'boolean': Boolean,
            'date': Date,
            'json': mongoose_1.Schema.Types.Mixed,
            'objectId': mongoose_1.Schema.Types.ObjectId
        };
        const schemaType = {
            type: typeMap[definition.type] || String,
            required: definition.required === true,
            unique: definition.unique === true,
            default: definition.default,
        };
        if (definition.ref) {
            schemaType.ref = definition.ref;
        }
        return schemaType;
    }
    _convertQuery(query) {
        const operatorMap = {
            $eq: '$eq',
            $ne: '$ne',
            $gt: '$gt',
            $gte: '$gte',
            $lt: '$lt',
            $lte: '$lte',
            $in: '$in',
            $nin: '$nin',
            $regex: '$regex',
            $exists: '$exists'
        };
        return this._processQueryOperators(query, operatorMap);
    }
    _processQueryOperators(query, operatorMap) {
        const processed = {};
        for (const [key, value] of Object.entries(query)) {
            if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
                const conditions = {};
                for (const [operator, operand] of Object.entries(value)) {
                    if (operatorMap[operator]) {
                        conditions[operatorMap[operator]] = operand;
                    }
                }
                processed[key] = Object.keys(conditions).length > 0 ? conditions : value;
            }
            else {
                processed[key] = value;
            }
        }
        return processed;
    }
    _convertUpdateOperations(update) {
        if (update.$set || update.$unset || update.$push || update.$pull) {
            return update;
        }
        return { $set: update };
    }
    _addSchemaIndexes(mongooseSchema, schema) {
        const indexes = [];
        for (const [field, definition] of Object.entries(schema)) {
            if (definition.index) {
                indexes.push({ [field]: 1 });
            }
        }
        if (indexes.length > 0) {
            mongooseSchema.index(indexes);
        }
    }
}
exports.default = MongoAdapter;
//# sourceMappingURL=MongoAdapter.js.map