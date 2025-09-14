"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const sequelize_1 = require("sequelize");
const BaseAdapter_1 = require("./BaseAdapter");
const errors_1 = require("../utils/errors");
class PostgresAdapter extends BaseAdapter_1.BaseAdapter {
    constructor(config) {
        super(config);
        this.models = new Map();
        this.sequelize = new sequelize_1.Sequelize({
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
    async initialize(options) {
        try {
            await this.sequelize.authenticate();
        }
        catch (error) {
            throw new errors_1.DatabaseError(`Postgres initialization failed: ${error.message}`);
        }
    }
    async create(model, data, options) {
        throw new Error('Not implemented');
    }
    async findOne(model, query, options) {
        throw new Error('Not implemented');
    }
    async findMany(model, query, options) {
        throw new Error('Not implemented');
    }
    async update(model, query, data, options) {
        throw new Error('Not implemented');
    }
    async delete(model, query, options) {
        throw new Error('Not implemented');
    }
    async transaction(callback, options) {
        throw new Error('Not implemented');
    }
    async close() {
        await this.sequelize.close();
    }
    async createModel(model, schema, options) {
        throw new Error('Not implemented');
    }
    async dropModel(model, options) {
        throw new Error('Not implemented');
    }
    async count(model, query, options) {
        throw new Error('Not implemented');
    }
    async exists(model, query, options) {
        throw new Error('Not implemented');
    }
    async bulkCreate(model, data, options) {
        throw new Error('Not implemented');
    }
    async bulkUpdate(model, updates, options) {
        throw new Error('Not implemented');
    }
    async aggregate(model, pipeline, options) {
        throw new Error('Not implemented');
    }
    async query(query, params, options) {
        throw new Error('Not implemented');
    }
}
exports.default = PostgresAdapter;
//# sourceMappingURL=PostgressAdapter.js.map