const { Database, aql } = require('arangojs');
const BaseAdapter = require('./BaseAdapter');
const { DatabaseError } = require('../utils/errors');

class ArangoAdapter extends BaseAdapter {
    constructor(config) {
        super();
        this.config = config;
        this.db = null;
        this.collections = new Map();
        this.graphs = new Map();
    }

    /**
     * Initialize ArangoDB connection
     */
    async initialize() {
        try {
            this.db = new Database({
                url: this.config.url,
                databaseName: this.config.database,
                auth: {
                    username: this.config.username,
                    password: this.config.password
                },
                agentOptions: {
                    maxSockets: 20,
                    keepAlive: true
                }
            });

            // Verify connection
            await this.db.version();
            return this.db;
        } catch (error) {
            throw new DatabaseError(`ArangoDB initialization failed: ${error.message}`);
        }
    }

    /**
     * Create collection
     * @param {string} name - Collection name
     * @param {Object} options - Collection options
     */
    async createCollection(name, options = {}) {
        try {
            const {
                type = 'document',
                keyOptions = { type: 'autoincrement', increment: 1 },
                schema = null
            } = options;

            let collection;
            if (type === 'edge') {
                collection = this.db.edgeCollection(name);
            } else {
                collection = this.db.collection(name);
            }

            await collection.create({
                keyOptions,
                waitForSync: true,
                ...options
            });

            if (schema) {
                await collection.properties({
                    schema
                });
            }

            this.collections.set(name, collection);
            return collection;
        } catch (error) {
            throw new DatabaseError(`Create collection failed: ${error.message}`);
        }
    }

    /**
     * Create graph
     * @param {string} name - Graph name
     * @param {Array} edgeDefinitions - Edge definitions
     */
    async createGraph(name, edgeDefinitions) {
        try {
            const graph = this.db.graph(name);
            await graph.create({
                edgeDefinitions: edgeDefinitions.map(def => ({
                    collection: def.collection,
                    from: Array.isArray(def.from) ? def.from : [def.from],
                    to: Array.isArray(def.to) ? def.to : [def.to]
                }))
            });

            this.graphs.set(name, graph);
            return graph;
        } catch (error) {
            throw new DatabaseError(`Create graph failed: ${error.message}`);
        }
    }

    /**
     * Insert document
     * @param {string} collection - Collection name
     * @param {Object} document - Document to insert
     */
    async insert(collection, document) {
        try {
            const coll = this._getCollection(collection);
            const result = await coll.save(document, {
                returnNew: true
            });

            return result.new;
        } catch (error) {
            throw new DatabaseError(`Insert failed: ${error.message}`);
        }
    }

    /**
     * Insert edge
     * @param {string} collection - Edge collection name
     * @param {string} from - From vertex ID
     * @param {string} to - To vertex ID
     * @param {Object} data - Edge data
     */
    async insertEdge(collection, from, to, data = {}) {
        try {
            const edgeCollection = this._getCollection(collection);
            const result = await edgeCollection.save({
                _from: from,
                _to: to,
                ...data
            }, {
                returnNew: true
            });

            return result.new;
        } catch (error) {
            throw new DatabaseError(`Insert edge failed: ${error.message}`);
        }
    }

    /**
     * Find documents
     * @param {string} collection - Collection name
     * @param {Object} query - Query filter
     * @param {Object} options - Query options
     */
    async find(collection, query = {}, options = {}) {
        try {
            const {
                limit,
                offset,
                sort,
                select
            } = options;

            let aqlQuery = aql`
                FOR doc IN ${this._getCollection(collection)}
            `;

            // Add filters
            if (Object.keys(query).length > 0) {
                aqlQuery = aql`
                    ${aqlQuery}
                    FILTER ${this._buildFilterExpression(query)}
                `;
            }

            // Add sort
            if (sort) {
                const sortFields = Object.entries(sort).map(([field, order]) => 
                    `doc.${field} ${order === -1 ? 'DESC' : 'ASC'}`
                );
                aqlQuery = aql`
                    ${aqlQuery}
                    SORT ${aql.join(sortFields, ', ')}
                `;
            }

            // Add projection
            if (select) {
                const projection = this._buildProjection(select);
                aqlQuery = aql`
                    ${aqlQuery}
                    RETURN ${projection}
                `;
            } else {
                aqlQuery = aql`
                    ${aqlQuery}
                    RETURN doc
                `;
            }

            // Add limit and offset
            if (limit) {
                aqlQuery = aql`
                    ${aqlQuery}
                    LIMIT ${offset || 0}, ${limit}
                `;
            }

            const cursor = await this.db.query(aqlQuery);
            return await cursor.all();
        } catch (error) {
            throw new DatabaseError(`Find failed: ${error.message}`);
        }
    }

    /**
     * Find one document
     * @param {string} collection - Collection name
     * @param {Object} query - Query filter
     * @param {Object} options - Query options
     */
    async findOne(collection, query = {}, options = {}) {
        try {
            const results = await this.find(collection, query, {
                ...options,
                limit: 1
            });
            return results[0] || null;
        } catch (error) {
            throw new DatabaseError(`Find one failed: ${error.message}`);
        }
    }

    /**
     * Update documents
     * @param {string} collection - Collection name
     * @param {Object} query - Query filter
     * @param {Object} update - Update operations
     * @param {Object} options - Update options
     */
    async update(collection, query, update, options = {}) {
        try {
            const {
                returnNew = true,
                returnOld = false,
                keepNull = true
            } = options;

            const updateOps = this._buildUpdateOperations(update);

            let aqlQuery = aql`
                FOR doc IN ${this._getCollection(collection)}
                FILTER ${this._buildFilterExpression(query)}
                UPDATE doc WITH ${updateOps} IN ${this._getCollection(collection)}
                OPTIONS { keepNull: ${keepNull} }
            `;

            if (returnNew && returnOld) {
                aqlQuery = aql`${aqlQuery} RETURN { new: NEW, old: OLD }`;
            } else if (returnNew) {
                aqlQuery = aql`${aqlQuery} RETURN NEW`;
            } else if (returnOld) {
                aqlQuery = aql`${aqlQuery} RETURN OLD`;
            }

            const cursor = await this.db.query(aqlQuery);
            return await cursor.all();
        } catch (error) {
            throw new DatabaseError(`Update failed: ${error.message}`);
        }
    }

    /**
     * Delete documents
     * @param {string} collection - Collection name
     * @param {Object} query - Query filter
     * @param {Object} options - Delete options
     */
    async delete(collection, query, options = {}) {
        try {
            const { returnOld = false } = options;

            let aqlQuery = aql`
                FOR doc IN ${this._getCollection(collection)}
                FILTER ${this._buildFilterExpression(query)}
                REMOVE doc IN ${this._getCollection(collection)}
            `;

            if (returnOld) {
                aqlQuery = aql`${aqlQuery} RETURN OLD`;
            }

            const cursor = await this.db.query(aqlQuery);
            return await cursor.all();
        } catch (error) {
            throw new DatabaseError(`Delete failed: ${error.message}`);
        }
    }

    /**
     * Execute AQL query
     * @param {string} query - AQL query
     * @param {Object} bindVars - Query variables
     */
    async query(query, bindVars = {}) {
        try {
            const cursor = await this.db.query(query, bindVars);
            return await cursor.all();
        } catch (error) {
            throw new DatabaseError(`Query failed: ${error.message}`);
        }
    }

    /**
     * Execute graph traversal
     * @param {string} startVertex - Start vertex ID
     * @param {Object} options - Traversal options
     */
    async traverse(startVertex, options = {}) {
        try {
            const {
                direction = 'outbound',
                minDepth = 1,
                maxDepth = 1,
                edgeCollection,
                vertexCollection
            } = options;

            const query = aql`
                FOR v, e, p IN ${minDepth}..${maxDepth} ${direction}
                ${startVertex}
                ${edgeCollection ? this._getCollection(edgeCollection) : null}
                ${vertexCollection ? this._getCollection(vertexCollection) : null}
                RETURN { vertex: v, edge: e, path: p }
            `;

            const cursor = await this.db.query(query);
            return await cursor.all();
        } catch (error) {
            throw new DatabaseError(`Traversal failed: ${error.message}`);
        }
    }

    /**
     * Execute transaction
     * @param {Function} action - Transaction action
     * @param {Object} options - Transaction options
     */
    async transaction(action, options = {}) {
        try {
            const result = await this.db.transaction(
                options.collections || {},
                action,
                options
            );
            return result;
        } catch (error) {
            throw new DatabaseError(`Transaction failed: ${error.message}`);
        }
    }

    /**
     * Close connection
     */
    async close() {
        try {
            await this.db.close();
        } catch (error) {
            throw new DatabaseError(`Close failed: ${error.message}`);
        }
    }

    // Private helper methods
    _getCollection(name) {
        const collection = this.collections.get(name) || this.db.collection(name);
        if (!collection) {
            throw new Error(`Collection ${name} not found`);
        }
        return collection;
    }

    _buildFilterExpression(query) {
        const conditions = [];
        
        for (const [key, value] of Object.entries(query)) {
            if (typeof value === 'object' && value !== null) {
                for (const [operator, operand] of Object.entries(value)) {
                    conditions.push(this._buildOperatorExpression(key, operator, operand));
                }
            } else {
                conditions.push(aql`doc.${key} == ${value}`);
            }
        }

        return conditions.length > 0 
            ? aql.join(conditions, ' AND ')
            : aql`true`;
    }

    _buildOperatorExpression(key, operator, value) {
        switch (operator) {
            case '$eq':
                return aql`doc.${key} == ${value}`;
            case '$ne':
                return aql`doc.${key} != ${value}`;
            case '$gt':
                return aql`doc.${key} > ${value}`;
            case '$gte':
                return aql`doc.${key} >= ${value}`;
            case '$lt':
                return aql`doc.${key} < ${value}`;
            case '$lte':
                return aql`doc.${key} <= ${value}`;
            case '$in':
                return aql`doc.${key} IN ${value}`;
            case '$nin':
                return aql`doc.${key} NOT IN ${value}`;
            case '$exists':
                return value ? aql`HAS(doc, ${key})` : aql`!HAS(doc, ${key})`;
            case '$regex':
                return aql`REGEX_TEST(doc.${key}, ${value}, true)`;
            default:
                throw new Error(`Unknown operator: ${operator}`);
        }
    }

    _buildProjection(select) {
        if (Array.isArray(select)) {
            const fields = select.map(field => `"${field}": doc.${field}`);
            return aql`{ ${aql.join(fields, ', ')} }`;
        }
        return aql`doc`;
    }

    _buildUpdateOperations(update) {
        const operations = {};

        for (const [key, value] of Object.entries(update)) {
            if (typeof value === 'object' && value !== null) {
                const operator = Object.keys(value)[0];
                const operand = value[operator];

                switch (operator) {
                    case '$set':
                        operations[key] = operand;
                        break;
                    case '$unset':
                        operations[key] = null;
                        break;
                    case '$inc':
                        operations[key] = aql`doc.${key} + ${operand}`;
                        break;
                    case '$push':
                        operations[key] = aql`APPEND(doc.${key}, ${operand})`;
                        break;
                    case '$pull':
                        operations[key] = aql`REMOVE_VALUES(doc.${key}, ${operand})`;
                        break;
                }
            } else {
                operations[key] = value;
            }
        }

        return operations;
    }
}

module.exports = ArangoAdapter;