// src/adapters/OrientDBAdapter.js
const OrientDBClient = require('orientjs').OrientDBClient;
const BaseAdapter = require('./BaseAdapter');
const { DatabaseError } = require('../utils/errors');

class OrientDBAdapter extends BaseAdapter {
    constructor(config) {
        super();
        this.config = config;
        this.server = null;
        this.db = null;
        this.classes = new Map();
    }

    /**
     * Initialize OrientDB connection
     */
    async initialize() {
        try {
            this.server = await OrientDBClient.connect({
                host: this.config.host,
                port: this.config.port,
                // servers?: OServerConfig[] | undefined;
                // pool?: { max?: number | undefined; min?: number | undefined } | undefined;
                // subscribePool?: { max?: number | undefined } | undefined;
                // logger?: any;
            });

            this.db = await this.server.session({
                name: this.config.database,
                username: this.config.dbUsername,
                password: this.config.dbPassword
            });

            return this.db;
        } catch (error) {
            throw new DatabaseError(`OrientDB initialization failed: ${error.message}`);
        }
    }

    /**
     * Create class (similar to table/collection)
     * @param {string} className - Class name
     * @param {Object} schema - Class schema
     * @param {Object} options - Class options
     */
    async createClass(className, schema, options = {}) {
        try {
            const {
                extends: parentClass = 'V',
                abstract = false,
                clusters = 1,
                indexes = []
            } = options;

            // Create class
            let createQuery = `CREATE CLASS ${className}`;
            if (parentClass) {
                createQuery += ` EXTENDS ${parentClass}`;
            }
            if (abstract) {
                createQuery += ' ABSTRACT';
            }
            if (clusters > 1) {
                createQuery += ` CLUSTERS ${clusters}`;
            }

            await this.db.query(createQuery);

            // Create properties
            for (const [propertyName, definition] of Object.entries(schema)) {
                await this.createProperty(className, propertyName, definition);
            }

            // Create indexes
            for (const index of indexes) {
                await this.createIndex(className, index);
            }

            this.classes.set(className, { schema, indexes });
            return true;
        } catch (error) {
            throw new DatabaseError(`Create class failed: ${error.message}`);
        }
    }

    /**
     * Create property for a class
     * @param {string} className - Class name
     * @param {string} propertyName - Property name
     * @param {Object} definition - Property definition
     */
    async createProperty(className, propertyName, definition) {
        try {
            let query = `CREATE PROPERTY ${className}.${propertyName} ${this._getOrientDBType(definition)}`;

            if (definition.linkedClass) {
                query += ` ${definition.linkedClass}`;
            }

            await this.db.query(query);

            // Set constraints
            if (definition.mandatory) {
                await this.db.query(`ALTER PROPERTY ${className}.${propertyName} MANDATORY true`);
            }
            if (definition.notNull) {
                await this.db.query(`ALTER PROPERTY ${className}.${propertyName} NOTNULL true`);
            }
            if (definition.readOnly) {
                await this.db.query(`ALTER PROPERTY ${className}.${propertyName} READONLY true`);
            }
            if (definition.min !== undefined) {
                await this.db.query(`ALTER PROPERTY ${className}.${propertyName} MIN ${definition.min}`);
            }
            if (definition.max !== undefined) {
                await this.db.query(`ALTER PROPERTY ${className}.${propertyName} MAX ${definition.max}`);
            }
            if (definition.regexp) {
                await this.db.query(`ALTER PROPERTY ${className}.${propertyName} REGEXP ${definition.regexp}`);
            }

            return true;
        } catch (error) {
            throw new DatabaseError(`Create property failed: ${error.message}`);
        }
    }

    /**
     * Create index
     * @param {string} className - Class name
     * @param {Object} index - Index definition
     */
    async createIndex(className, index) {
        try {
            const {
                name,
                type = 'UNIQUE',
                properties,
                engine = 'SBTREE'
            } = index;

            const indexName = name || `${className}_${Array.isArray(properties) ? properties.join('_') : properties}`;
            const propertyList = Array.isArray(properties) ? properties.join(', ') : properties;

            await this.db.query(
                `CREATE INDEX ${indexName} ON ${className} (${propertyList}) ${type} ${engine}`
            );

            return true;
        } catch (error) {
            throw new DatabaseError(`Create index failed: ${error.message}`);
        }
    }

    /**
     * Create vertex
     * @param {string} className - Vertex class name
     * @param {Object} data - Vertex data
     */
    async createVertex(className, data) {
        try {
            const formattedData = this._formatData(data, this.classes.get(className).schema);
            const vertex = await this.db.create('VERTEX', className)
                .set(formattedData)
                .one();

            return this._processRecord(vertex);
        } catch (error) {
            throw new DatabaseError(`Create vertex failed: ${error.message}`);
        }
    }

    /**
     * Create edge
     * @param {string} className - Edge class name
     * @param {string} fromVertex - From vertex ID or RID
     * @param {string} toVertex - To vertex ID or RID
     * @param {Object} data - Edge data
     */
    async createEdge(className, fromVertex, toVertex, data = {}) {
        try {
            const formattedData = this._formatData(data, this.classes.get(className).schema);
            const edge = await this.db.create('EDGE', className)
                .from(fromVertex)
                .to(toVertex)
                .set(formattedData)
                .one();

            return this._processRecord(edge);
        } catch (error) {
            throw new DatabaseError(`Create edge failed: ${error.message}`);
        }
    }

    /**
     * Find vertices
     * @param {string} className - Vertex class name
     * @param {Object} query - Query conditions
     * @param {Object} options - Query options
     */
    async findVertices(className, query = {}, options = {}) {
        try {
            const {
                select,
                orderBy,
                limit,
                skip
            } = options;

            let queryStr = `SELECT ${select || ''} FROM ${className}`;
            const params = {};

            if (Object.keys(query).length > 0) {
                const whereClause = this._buildWhereClause(query);
                queryStr += ` WHERE ${whereClause.condition}`;
                Object.assign(params, whereClause.params);
            }

            if (orderBy) {
                queryStr += ` ORDER BY ${this._buildOrderByClause(orderBy)}`;
            }

            if (limit) {
                queryStr += ` LIMIT ${limit}`;
            }

            if (skip) {
                queryStr += ` SKIP ${skip}`;
            }

            console.log('Executing query:', queryStr, params);

            return await this.query(queryStr, { params });
        } catch (error) {
            throw new DatabaseError(`Find vertices failed: ${error.message}`);
        }
    }

    /**
     * Find edges
     * @param {string} className - Edge class name
     * @param {Object} query - Query conditions
     * @param {Object} options - Query options
     */
    async findEdges(className, query = {}, options = {}) {
        try {
            const {
                select,
                orderBy,
                limit,
                skip
            } = options;

            let queryStr = `SELECT ${select || '*'} FROM ${className}`;
            const params = {};

            if (Object.keys(query).length > 0) {
                const whereClause = this._buildWhereClause(query);
                queryStr += ` WHERE ${whereClause.condition}`;
                Object.assign(params, whereClause.params);
            }

            if (orderBy) {
                queryStr += ` ORDER BY ${this._buildOrderByClause(orderBy)}`;
            }

            if (limit) {
                queryStr += ` LIMIT ${limit}`;
            }

            if (skip) {
                queryStr += ` SKIP ${skip}`;
            }

            const results = await this.db.query(queryStr, { params });
            return results.map(record => this._processRecord(record));
        } catch (error) {
            throw new DatabaseError(`Find edges failed: ${error.message}`);
        }
    }

    /**
     * Traverse graph
     * @param {string} startVertex - Starting vertex ID or RID
     * @param {Object} options - Traversal options
     */
    async traverse(startVertex, options = {}) {
        try {
            const {
                direction = 'OUT',
                edgeClass = null,
                vertexClass = null,
                maxDepth = 1,
                strategy = 'BREADTH_FIRST'
            } = options;

            let queryStr = `TRAVERSE ${direction}('${edgeClass || ''}') FROM ${startVertex}`;

            if (vertexClass) {
                queryStr += ` WHILE @class = '${vertexClass}'`;
            }

            queryStr += ` MAXDEPTH ${maxDepth} STRATEGY ${strategy}`;

            const results = await this.db.query(queryStr);
            return results.map(record => this._processRecord(record));
        } catch (error) {
            throw new DatabaseError(`Traverse failed: ${error.message}`);
        }
    }

    /**
     * Find shortest path
     * @param {string} fromVertex - Starting vertex ID or RID
     * @param {string} toVertex - Target vertex ID or RID
     * @param {Object} options - Path options
     */
    async findShortestPath(fromVertex, toVertex, options = {}) {
        try {
            const {
                direction = 'OUT',
                edgeClass = null,
                maxDepth = 10
            } = options;

            let queryStr = `SELECT shortestPath(${fromVertex}, ${toVertex}, '${direction}'`;

            if (edgeClass) {
                queryStr += `, '${edgeClass}'`;
            }

            queryStr += `, ${maxDepth})`;

            const results = await this.db.query(queryStr);
            return results.map(record => this._processRecord(record));
        } catch (error) {
            throw new DatabaseError(`Find shortest path failed: ${error.message}`);
        }
    }

    /**
     * Execute transaction
     * @param {Function} callback - Transaction callback
     */
    async transaction(callback) {
        const tx = await this.db.begin();
        try {
            const result = await callback(tx);
            await tx.commit();
            return result;
        } catch (error) {
            await tx.rollback();
            throw new DatabaseError(`Transaction failed: ${error.message}`);
        }
    }

    /**
     * Execute custom query
     * @param {string} query - OrientDB SQL query
     * @param {Object} params - Query parameters
     */
    async query(query, params = {}) {
        try {
            this.db.query(query, { params })
                .on("data", data => {
                    console.log(data);
                    return data;
                })
                .on('error', (err) => {
                    console.log('orientdb stream error');
                    return err;
                })
                .on("end", () => {
                    console.log('orientdb stream end');
                    return "End of the stream";
                });
        } catch (error) {
            throw new DatabaseError(`Query failed: ${error.message}`);
        }
    }

    /**
     * Close connection
     */
    async close() {
        try {
            await this.db.close();
            await this.server.close();
        } catch (error) {
            throw new DatabaseError(`Close failed: ${error.message}`);
        }
    }

    // Continuation of OrientDBAdapter class private methods

    /**
     * Get OrientDB data type
     * @private
     */
    _getOrientDBType(definition) {
        const typeMap = {
            'string': 'STRING',
            'number': 'DOUBLE',
            'integer': 'INTEGER',
            'boolean': 'BOOLEAN',
            'date': 'DATETIME',
            'binary': 'BINARY',
            'byte': 'BYTE',
            'short': 'SHORT',
            'long': 'LONG',
            'float': 'FLOAT',
            'double': 'DOUBLE',
            'decimal': 'DECIMAL',
            'list': 'EMBEDDEDLIST',
            'set': 'EMBEDDEDSET',
            'map': 'EMBEDDEDMAP',
            'link': 'LINK',
            'linkList': 'LINKLIST',
            'linkSet': 'LINKSET',
            'linkMap': 'LINKMAP',
            'embedded': 'EMBEDDED',
            'embeddedList': 'EMBEDDEDLIST',
            'embeddedSet': 'EMBEDDEDSET',
            'embeddedMap': 'EMBEDDEDMAP'
        };

        let type = typeMap[definition.type] || 'STRING';

        if (definition.type === 'list' || definition.type === 'set' || definition.type === 'map') {
            if (definition.linkedClass) {
                type = type.replace('EMBEDDED', 'LINK');
            }
        }

        return type;
    }

    /**
     * Format data for database storage
     * @private
     */
    _formatData(data, schema) {
        const formatted = {};

        for (const [key, value] of Object.entries(data)) {
            if (value !== undefined && schema[key]) {
                formatted[key] = this._formatValue(value, schema[key]);
            }
        }

        return formatted;
    }

    /**
     * Format individual value based on type
     * @private
     */
    _formatValue(value, definition) {
        if (value === null || value === undefined) {
            return null;
        }

        switch (definition.type) {
            case 'date':
                return value instanceof Date ? value : new Date(value);
            case 'list':
            case 'set':
                return Array.isArray(value) ? value.map(v => this._formatValue(v, { type: definition.itemType })) : [value];
            case 'map':
                if (typeof value === 'object') {
                    const formatted = {};
                    for (const [k, v] of Object.entries(value)) {
                        formatted[k] = this._formatValue(v, { type: definition.valueType });
                    }
                    return formatted;
                }
                return value;
            case 'embedded':
                return typeof value === 'object' ? this._formatData(value, definition.schema) : value;
            case 'link':
            case 'linkList':
            case 'linkSet':
            case 'linkMap':
                return value; // RIDs are handled by OrientDB
            default:
                return value;
        }
    }

    /**
     * Process database record
     * @private
     */
    _processRecord(record) {
        if (!record) return null;

        const processed = {};
        for (const [key, value] of Object.entries(record)) {
            if (key !== '@class' && key !== '@version' && key !== '@type') {
                processed[key] = this._processValue(value);
            } else if (key === '@class') {
                processed._class = value;
            }
        }

        if (record['@rid']) {
            processed._id = record['@rid'].toString();
        }

        return processed;
    }

    /**
     * Process individual value from database
     * @private
     */
    _processValue(value) {
        if (value === null || value === undefined) {
            return null;
        }

        if (Array.isArray(value)) {
            return value.map(v => this._processValue(v));
        }

        if (typeof value === 'object') {
            if (value instanceof Date) {
                return value;
            }
            if (value['@type'] === 'd') {
                return new Date(value.value);
            }
            if (value['@rid']) {
                return value['@rid'].toString();
            }
            if (value['@class']) {
                return this._processRecord(value);
            }

            const processed = {};
            for (const [k, v] of Object.entries(value)) {
                processed[k] = this._processValue(v);
            }
            return processed;
        }

        return value;
    }

    /**
     * Build WHERE clause
     * @private
     */
    _buildWhereClause(query) {
        const conditions = [];
        const params = {};
        let paramCounter = 0;

        for (const [key, value] of Object.entries(query)) {
            if (typeof value === 'object' && value !== null) {
                for (const [operator, operand] of Object.entries(value)) {
                    const { condition, parameter } = this._buildOperatorCondition(
                        key,
                        operator,
                        operand,
                        paramCounter++
                    );
                    conditions.push(condition);
                    if (parameter !== undefined) {
                        params[`p${paramCounter}`] = parameter;
                    }
                }
            } else {
                conditions.push(`${key} = :p${paramCounter}`);
                params[`p${paramCounter}`] = value;
                paramCounter++;
            }
        }

        return {
            condition: conditions.length > 0 ? conditions.join(' AND ') : '1=1',
            params
        };
    }

    /**
     * Build operator condition
     * @private
     */
    _buildOperatorCondition(field, operator, value, counter) {
        const paramName = `p${counter}`;

        switch (operator) {
            case '$eq':
                return {
                    condition: `${field} = :${paramName}`,
                    parameter: value
                };
            case '$ne':
                return {
                    condition: `${field} <> :${paramName}`,
                    parameter: value
                };
            case '$gt':
                return {
                    condition: `${field} > :${paramName}`,
                    parameter: value
                };
            case '$gte':
                return {
                    condition: `${field} >= :${paramName}`,
                    parameter: value
                };
            case '$lt':
                return {
                    condition: `${field} < :${paramName}`,
                    parameter: value
                };
            case '$lte':
                return {
                    condition: `${field} <= :${paramName}`,
                    parameter: value
                };
            case '$in':
                return {
                    condition: `${field} IN :${paramName}`,
                    parameter: Array.isArray(value) ? value : [value]
                };
            case '$nin':
                return {
                    condition: `${field} NOT IN :${paramName}`,
                    parameter: Array.isArray(value) ? value : [value]
                };
            case '$contains':
                return {
                    condition: `${field} CONTAINS :${paramName}`,
                    parameter: value
                };
            case '$containsAll':
                return {
                    condition: `${field} CONTAINSALL :${paramName}`,
                    parameter: Array.isArray(value) ? value : [value]
                };
            case '$containsText':
                return {
                    condition: `${field} CONTAINSTEXT :${paramName}`,
                    parameter: value
                };
            case '$between':
                return {
                    condition: `${field} BETWEEN :${paramName}_start AND :${paramName}_end`,
                    parameter: { [`${paramName}_start`]: value[0], [`${paramName}_end`]: value[1] }
                };
            case '$matches':
                return {
                    condition: `${field} MATCHES :${paramName}`,
                    parameter: value
                };
            case '$like':
                return {
                    condition: `${field} LIKE :${paramName}`,
                    parameter: value
                };
            default:
                throw new Error(`Unknown operator: ${operator}`);
        }
    }

    /**
     * Build ORDER BY clause
     * @private
     */
    _buildOrderByClause(orderBy) {
        return Object.entries(orderBy)
            .map(([field, order]) => `${field} ${order === -1 ? 'DESC' : 'ASC'}`)
            .join(', ');
    }
}

module.exports = OrientDBAdapter;