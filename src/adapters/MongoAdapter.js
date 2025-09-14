
const mongoose = require('mongoose');
const { BaseAdapter } = require('./BaseAdapter');
const { DatabaseError } = require('../utils/errors');

class MongoAdapter extends BaseAdapter {
    constructor(config) {
        super();
        this.config = config;
        this.models = new Map();
        this.connection = null;
    }

    /**
     * Initialize MongoDB connection
     */
    async initialize() {
        try {
            this.connection = await mongoose.connect(this.config.uri);
            // Connection error handling
            this.connection.on('error', (error) => {
                throw new DatabaseError(`MongoDB connection error: ${error.message}`);
            });

            return this.connection;
        } catch (error) {
            throw new DatabaseError(`MongoDB initialization failed: ${error.message}`);
        }
    }

    /**
     * Define a new model
     * @param {string} modelName - Name of the model
     * @param {Object} schema - Schema definition
     * @returns {Object} Mongoose model
     */
    defineModel(modelName, schema) {
        try {
            if (!this.connection) {
                throw new Error('Database connection not initialized');
            }

            const mongooseSchema = new mongoose.Schema(
                this._convertSchema(schema),
                { 
                    timestamps: true,
                    strict: true
                }
            );

            // Add indexes
            this._addSchemaIndexes(mongooseSchema, schema);

            // Add middleware hooks
            this._addSchemaMiddleware(mongooseSchema);

            const model = this.connection.model(modelName, mongooseSchema);
            this.models.set(modelName, model);
            return model;
        } catch (error) {
            throw new DatabaseError(`Failed to define model: ${error.message}`);
        }
    }

    /**
     * Create a new document
     * @param {string} modelName - Name of the model
     * @param {Object} data - Data to create
     * @param {Object} options - Additional options
     * @returns {Object} Created document
     */
    async create(modelName, data, options = {}) {
        try {
            const model = this._getModel(modelName);
            const result = await model.create([data], options);
            return result[0].toObject();
        } catch (error) {
            throw new DatabaseError(`Create operation failed: ${error.message}`);
        }
    }

    /**
     * Find a single document
     * @param {string} modelName - Name of the model
     * @param {Object} query - Query conditions
     * @param {Object} options - Additional options
     * @returns {Object} Found document
     */
    async findOne(modelName, query, options = {}) {
        try {
            const model = this._getModel(modelName);
            const { select, populate } = options;
            
            let mongoQuery = model.findOne(this._convertQuery(query));
            
            if (select) mongoQuery = mongoQuery.select(select);
            if (populate) mongoQuery = mongoQuery.populate(populate);
            
            const result = await mongoQuery.exec();
            return result ? result.toObject() : null;
        } catch (error) {
            throw new DatabaseError(`Find operation failed: ${error.message}`);
        }
    }

    /**
     * Find multiple documents
     * @param {string} modelName - Name of the model
     * @param {Object} query - Query options
     * @returns {Array} Found documents
     */
    async findMany(modelName, query = {}) {
        try {
            const model = this._getModel(modelName);
            const { 
                filter = {}, 
                select, 
                populate,
                limit,
                skip,
                sort
            } = query;

            let mongoQuery = model.find(this._convertQuery(filter));
            
            if (select) mongoQuery = mongoQuery.select(select);
            if (populate) mongoQuery = mongoQuery.populate(populate);
            if (limit) mongoQuery = mongoQuery.limit(parseInt(limit));
            if (skip) mongoQuery = mongoQuery.skip(parseInt(skip));
            if (sort) mongoQuery = mongoQuery.sort(sort);

            const results = await mongoQuery.exec();
            return results.map(doc => doc.toObject());
        } catch (error) {
            throw new DatabaseError(`Find operation failed: ${error.message}`);
        }
    }

    /**
     * Update documents
     * @param {string} modelName - Name of the model
     * @param {Object} query - Query conditions
     * @param {Object} data - Update data
     * @param {Object} options - Additional options
     * @returns {Object} Update result
     */
    async update(modelName, query, data, options = {}) {
        try {
            const model = this._getModel(modelName);
            const result = await model.updateMany(
                this._convertQuery(query),
                this._convertUpdateOperations(data),
                { ...options, new: true }
            );
            return result;
        } catch (error) {
            throw new DatabaseError(`Update operation failed: ${error.message}`);
        }
    }

    /**
     * Delete documents
     * @param {string} modelName - Name of the model
     * @param {Object} query - Query conditions
     * @returns {Object} Delete result
     */
    async delete(modelName, query) {
        try {
            const model = this._getModel(modelName);
            return await model.deleteMany(this._convertQuery(query));
        } catch (error) {
            throw new DatabaseError(`Delete operation failed: ${error.message}`);
        }
    }

    /**
     * Execute a transaction
     * @param {Function} callback - Transaction callback
     * @returns {*} Transaction result
     */
    async transaction(callback) {
        const session = await this.connection.startSession();
        try {
            session.startTransaction();
            const result = await callback(session);
            await session.commitTransaction();
            return result;
        } catch (error) {
            await session.abortTransaction();
            throw new DatabaseError(`Transaction failed: ${error.message}`);
        } finally {
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
            'json': mongoose.Schema.Types.Mixed,
            'objectId': mongoose.Schema.Types.ObjectId
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
            if (typeof value === 'object' && !Array.isArray(value)) {
                const conditions = {};
                for (const [operator, operand] of Object.entries(value)) {
                    if (operatorMap[operator]) {
                        conditions[operatorMap[operator]] = operand;
                    }
                }
                processed[key] = Object.keys(conditions).length > 0 ? conditions : value;
            } else {
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

    _addSchemaMiddleware(mongooseSchema) {
        // Add any common middleware here
        mongooseSchema.pre('save', function(next) {
            // Custom validation or modification logic
            next();
        });
    }
}

module.exports = MongoAdapter;