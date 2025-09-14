
const { Sequelize, Model, DataTypes } = require('sequelize');
const { BaseAdapter } = require('./BaseAdapter');
const { DatabaseError } = require('../utils/errors');

class PostgresAdapter extends BaseAdapter {
    constructor(config) {
        super();
        this.config = config;
        this.models = new Map();
        this.sequelize = new Sequelize({
            dialect: 'postgres',
            host: config.host,
            port: config.port,
            database: config.database,
            username: config.user,
            password: config.password,
            logging: config.logging || false,
            pool: {
                max: 5,
                min: 0,
                acquire: 30000,
                idle: 10000
            }
        });
    }

    /**
     * Define a new model
     * @param {string} modelName - Name of the model
     * @param {Object} schema - Schema definition
     * @returns {Object} Sequelize model
     */
    defineModel(modelName, schema) {
        try {
            // Convert our universal schema format to Sequelize format
            const sequelizeSchema = this._convertSchema(schema);
            
            class DynamicModel extends Model {}
            
            DynamicModel.init(sequelizeSchema, {
                sequelize: this.sequelize,
                modelName: modelName,
                timestamps: true
            });

            this.models.set(modelName, DynamicModel);
            return DynamicModel;
        } catch (error) {
            throw new DatabaseError(`Failed to define model: ${error.message}`);
        }
    }

    /**
     * Create a new record
     * @param {string} modelName - Name of the model
     * @param {Object} data - Data to create
     * @returns {Object} Created record
     */
    async create(modelName, data) {
        try {
            const model = this.models.get(modelName);
            if (!model) throw new Error(`Model ${modelName} not found`);
            
            const result = await model.create(data);
            return result.toJSON();
        } catch (error) {
            throw new DatabaseError(`Create failed: ${error.message}`);
        }
    }

    /**
     * Find a single record
     * @param {string} modelName - Name of the model
     * @param {Object} query - Query conditions
     * @returns {Object} Found record
     */
    async findOne(modelName, query) {
        try {
            const model = this.models.get(modelName);
            if (!model) throw new Error(`Model ${modelName} not found`);

            const result = await model.findOne({
                where: this._convertQuery(query)
            });
            return result ? result.toJSON() : null;
        } catch (error) {
            throw new DatabaseError(`Find operation failed: ${error.message}`);
        }
    }

    /**
     * Find multiple records
     * @param {string} modelName - Name of the model
     * @param {Object} query - Query conditions
     * @returns {Array} Found records
     */
    async findMany(modelName, query = {}) {
        try {
            const model = this.models.get(modelName);
            if (!model) throw new Error(`Model ${modelName} not found`);

            const { where, limit, offset, order } = this._processQueryOptions(query);
            
            const results = await model.findAll({
                where,
                limit,
                offset,
                order
            });

            return results.map(result => result.toJSON());
        } catch (error) {
            throw new DatabaseError(`Find operation failed: ${error.message}`);
        }
    }

    /**
     * Update records
     * @param {string} modelName - Name of the model
     * @param {Object} query - Query conditions
     * @param {Object} data - Data to update
     * @returns {number} Number of updated records
     */
    async update(modelName, query, data) {
        try {
            const model = this.models.get(modelName);
            if (!model) throw new Error(`Model ${modelName} not found`);

            const [count] = await model.update(data, {
                where: this._convertQuery(query)
            });
            return count;
        } catch (error) {
            throw new DatabaseError(`Update failed: ${error.message}`);
        }
    }

    /**
     * Delete records
     * @param {string} modelName - Name of the model
     * @param {Object} query - Query conditions
     * @returns {number} Number of deleted records
     */
    async delete(modelName, query) {
        try {
            const model = this.models.get(modelName);
            if (!model) throw new Error(`Model ${modelName} not found`);

            return await model.destroy({
                where: this._convertQuery(query)
            });
        } catch (error) {
            throw new DatabaseError(`Delete failed: ${error.message}`);
        }
    }

    /**
     * Execute a transaction
     * @param {Function} callback - Transaction callback
     * @returns {*} Transaction result
     */
    async transaction(callback) {
        try {
            return await this.sequelize.transaction(async (t) => {
                return await callback(t);
            });
        } catch (error) {
            throw new DatabaseError(`Transaction failed: ${error.message}`);
        }
    }

    /**
     * Close the database connection
     */
    async close() {
        await this.sequelize.close();
    }

    // Private helper methods
    _convertSchema(schema) {
        const sequelizeSchema = {};
        for (const [field, definition] of Object.entries(schema)) {
            sequelizeSchema[field] = this._convertFieldType(definition);
        }
        return sequelizeSchema;
    }

    _convertFieldType(definition) {
        const typeMap = {
            'string': DataTypes.STRING,
            'number': DataTypes.DOUBLE,
            'integer': DataTypes.INTEGER,
            'boolean': DataTypes.BOOLEAN,
            'date': DataTypes.DATE,
            'json': DataTypes.JSONB
        };

        const sequelizeField = {
            type: typeMap[definition.type] || DataTypes.STRING,
            allowNull: definition.required === false,
            unique: definition.unique === true,
            defaultValue: definition.default
        };

        if (definition.primaryKey) {
            sequelizeField.primaryKey = true;
            sequelizeField.autoIncrement = true;
        }

        return sequelizeField;
    }

    _convertQuery(query) {
        const { Op } = Sequelize;
        const operatorMap = {
            $eq: Op.eq,
            $ne: Op.ne,
            $gt: Op.gt,
            $gte: Op.gte,
            $lt: Op.lt,
            $lte: Op.lte,
            $in: Op.in,
            $nin: Op.notIn,
            $like: Op.like,
            $ilike: Op.iLike
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
                processed[key] = conditions;
            } else {
                processed[key] = value;
            }
        }

        return processed;
    }

    _processQueryOptions(query) {
        const { filter = {}, limit, offset, sort } = query;
        
        const options = {
            where: this._convertQuery(filter),
            limit: limit ? parseInt(limit) : undefined,
            offset: offset ? parseInt(offset) : undefined
        };

        if (sort) {
            options.order = Object.entries(sort).map(([field, order]) => [
                field,
                order.toLowerCase() === 'desc' ? 'DESC' : 'ASC'
            ]);
        }

        return options;
    }
}

module.exports = PostgresAdapter;