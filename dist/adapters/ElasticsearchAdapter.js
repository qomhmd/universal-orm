"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const BaseAdapter_1 = require("./BaseAdapter");
class ElasticsearchAdapter extends BaseAdapter_1.BaseAdapter {
    constructor(config) {
        super(config);
    }
    async initialize(options) {
        throw new Error('Not implemented');
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
        throw new Error('Not implemented');
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
exports.default = ElasticsearchAdapter;
//# sourceMappingURL=ElasticsearchAdapter.js.map