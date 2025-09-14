
const { Client } = require('@elastic/elasticsearch');
const BaseAdapter = require('./BaseAdapter');
const { DatabaseError } = require('../utils/errors');

class ElasticsearchAdapter extends BaseAdapter {
    constructor(config) {
        super();
        this.config = config;
        this.client = null;
        this.indices = new Map();
    }

    /**
     * Initialize Elasticsearch connection
     */
    async initialize() {
        try {
            this.client = new Client({
                node: this.config.node,
                auth: {
                    username: this.config.username,
                    password: this.config.password
                },
                ssl: {
                    rejectUnauthorized: this.config.rejectUnauthorized !== false
                }
            });

            await this.client.ping();
            return this.client;
        } catch (error) {
            throw new DatabaseError(`Elasticsearch initialization failed: ${error.message}`);
        }
    }

    /**
     * Create or update an index with mappings
     * @param {string} indexName - Name of the index
     * @param {Object} mappings - Index mappings
     */
    async createIndex(indexName, mappings) {
        try {
            const exists = await this.client.indices.exists({
                index: indexName
            });

            if (!exists) {
                await this.client.indices.create({
                    index: indexName,
                    body: {
                        mappings: this._convertMappings(mappings)
                    }
                });
            }

            this.indices.set(indexName, mappings);
        } catch (error) {
            throw new DatabaseError(`Failed to create index: ${error.message}`);
        }
    }

    /**
     * Index a document
     * @param {string} index - Index name
     * @param {Object} document - Document to index
     * @param {Object} options - Indexing options
     */
    async index(index, document, options = {}) {
        try {
            const { id, refresh = 'wait_for' } = options;
            
            const params = {
                index,
                refresh,
                body: document
            };

            if (id) {
                params.id = id;
            }

            const result = await this.client.index(params);
            return {
                id: result._id,
                result: result.result,
                version: result._version
            };
        } catch (error) {
            throw new DatabaseError(`Indexing failed: ${error.message}`);
        }
    }

    /**
     * Bulk index documents
     * @param {string} index - Index name
     * @param {Array} documents - Array of documents
     * @param {Object} options - Bulk indexing options
     */
    async bulkIndex(index, documents, options = {}) {
        try {
            const { refresh = 'wait_for' } = options;
            const operations = documents.flatMap(doc => [
                { index: { _index: index, _id: doc.id } },
                doc
            ]);

            const result = await this.client.bulk({
                refresh,
                body: operations
            });

            return result.items.map(item => ({
                id: item.index._id,
                result: item.index.result,
                status: item.index.status,
                error: item.index.error
            }));
        } catch (error) {
            throw new DatabaseError(`Bulk indexing failed: ${error.message}`);
        }
    }

    /**
     * Search documents
     * @param {string} index - Index name
     * @param {Object} query - Search query
     * @returns {Object} Search results
     */
    async search(index, query) {
        try {
            const searchParams = this._buildSearchQuery(query);
            searchParams.index = index;

            const response = await this.client.search(searchParams);
            
            return {
                hits: response.hits.hits.map(hit => ({
                    id: hit._id,
                    score: hit._score,
                    ...hit._source
                })),
                total: response.hits.total.value,
                aggregations: response.aggregations
            };
        } catch (error) {
            throw new DatabaseError(`Search failed: ${error.message}`);
        }
    }

    /**
     * Get document by ID
     * @param {string} index - Index name
     * @param {string} id - Document ID
     */
    async get(index, id) {
        try {
            const result = await this.client.get({
                index,
                id
            });
            
            return {
                id: result._id,
                ...result._source
            };
        } catch (error) {
            if (error.meta?.statusCode === 404) {
                return null;
            }
            throw new DatabaseError(`Get operation failed: ${error.message}`);
        }
    }

    /**
     * Update document
     * @param {string} index - Index name
     * @param {string} id - Document ID
     * @param {Object} update - Update data
     */
    async update(index, id, update, options = {}) {
        try {
            const { refresh = 'wait_for' } = options;
            
            const result = await this.client.update({
                index,
                id,
                refresh,
                body: {
                    doc: update
                }
            });

            return {
                id: result._id,
                result: result.result,
                version: result._version
            };
        } catch (error) {
            throw new DatabaseError(`Update failed: ${error.message}`);
        }
    }

    /**
     * Delete document
     * @param {string} index - Index name
     * @param {string} id - Document ID
     */
    async delete(index, id, options = {}) {
        try {
            const { refresh = 'wait_for' } = options;
            
            const result = await this.client.delete({
                index,
                id,
                refresh
            });

            return {
                id: result._id,
                result: result.result
            };
        } catch (error) {
            throw new DatabaseError(`Delete failed: ${error.message}`);
        }
    }

    /**
     * Delete by query
     * @param {string} index - Index name
     * @param {Object} query - Query to match documents for deletion
     */
    async deleteByQuery(index, query) {
        try {
            const result = await this.client.deleteByQuery({
                index,
                refresh: true,
                body: {
                    query: this._buildQuery(query)
                }
            });

            return {
                deleted: result.deleted,
                total: result.total
            };
        } catch (error) {
            throw new DatabaseError(`Delete by query failed: ${error.message}`);
        }
    }

    /**
     * Close connection
     */
    async close() {
        try {
            await this.client.close();
        } catch (error) {
            throw new DatabaseError(`Close failed: ${error.message}`);
        }
    }

    // Private helper methods
    _convertMappings(mappings) {
        const converted = {
            properties: {}
        };

        for (const [field, definition] of Object.entries(mappings)) {
            converted.properties[field] = this._convertFieldMapping(definition);
        }

        return converted;
    }

    _convertFieldMapping(definition) {
        const mapping = {
            type: this._getElasticsearchType(definition.type)
        };

        if (definition.analyzer) {
            mapping.analyzer = definition.analyzer;
        }

        if (definition.fields) {
            mapping.fields = definition.fields;
        }

        return mapping;
    }

    _getElasticsearchType(type) {
        const typeMap = {
            'text': 'text',
            'keyword': 'keyword',
            'long': 'long',
            'integer': 'integer',
            'short': 'short',
            'byte': 'byte',
            'double': 'double',
            'float': 'float',
            'boolean': 'boolean',
            'date': 'date',
            'object': 'object',
            'nested': 'nested'
        };

        return typeMap[type] || 'keyword';
    }

    _buildSearchQuery(params) {
        const {
            query,
            filter,
            sort,
            from,
            size,
            aggs,
            highlight,
            _source
        } = params;

        const searchBody = {};

        if (query || filter) {
            searchBody.query = this._buildQuery(query, filter);
        }

        if (sort) {
            searchBody.sort = this._buildSort(sort);
        }

        if (aggs) {
            searchBody.aggs = this._buildAggs(aggs);
        }

        if (highlight) {
            searchBody.highlight = this._buildHighlight(highlight);
        }

        if (_source) {
            searchBody._source = _source;
        }

        return {
            from: from || 0,
            size: size || 10,
            body: searchBody
        };
    }

    _buildQuery(query, filter) {
        if (filter) {
            return {
                bool: {
                    must: query ? this._buildQueryClause(query) : { match_all: {} },
                    filter: this._buildFilterClauses(filter)
                }
            };
        }

        return query ? this._buildQueryClause(query) : { match_all: {} };
    }

    _buildQueryClause(query) {
        if (query.match) {
            return { match: query.match };
        }
        if (query.match_phrase) {
            return { match_phrase: query.match_phrase };
        }
        if (query.multi_match) {
            return { 
                multi_match: {
                    query: query.multi_match.query,
                    fields: query.multi_match.fields
                }
            };
        }
        return query;
    }

    _buildFilterClauses(filter) {
        const clauses = [];

        for (const [field, conditions] of Object.entries(filter)) {
            if (typeof conditions === 'object') {
                for (const [operator, value] of Object.entries(conditions)) {
                    clauses.push(this._buildFilterClause(field, operator, value));
                }
            } else {
                clauses.push({ term: { [field]: conditions } });
            }
        }

        return clauses;
    }

    _buildFilterClause(field, operator, value) {
        const operatorMap = {
            $eq: { term: { [field]: value } },
            $ne: { bool: { must_not: { term: { [field]: value } } } },
            $gt: { range: { [field]: { gt: value } } },
            $gte: { range: { [field]: { gte: value } } },
            $lt: { range: { [field]: { lt: value } } },
            $lte: { range: { [field]: { lte: value } } },
            $in: { terms: { [field]: value } },
            $nin: { bool: { must_not: { terms: { [field]: value } } } }
        };

        return operatorMap[operator] || { term: { [field]: value } };
    }

    _buildSort(sort) {
        return Object.entries(sort).map(([field, order]) => ({
            [field]: { order: order.toLowerCase() }
        }));
    }

    _buildAggs(aggs) {
        const result = {};

        for (const [name, agg] of Object.entries(aggs)) {
            result[name] = this._buildAggClause(agg);
        }

        return result;
    }

    _buildAggClause(agg) {
        const { type, field, options = {} } = agg;
        return {
            [type]: {
                field,
                ...options
            }
        };
    }

    _buildHighlight(highlight) {
        const { fields, ...options } = highlight;
        return {
            fields: Object.fromEntries(
                fields.map(field => [field, {}])
            ),
            ...options
        };
    }
}

module.exports = ElasticsearchAdapter;