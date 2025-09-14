
const cassandra = require('cassandra-driver');
const { BaseAdapter } = require('./BaseAdapter');
const { DatabaseError } = require('../utils/errors');

class CassandraAdapter extends BaseAdapter {
    constructor(config) {
        super();
        this.config = config;
        this.client = null;
        this.keyspace = config.keyspace;
        this.models = new Map();
        this.preparedStatements = new Map();
    }

    /**
     * Initialize Cassandra connection
     */
    async initialize() {
        try {
            this.client = new cassandra.Client({
                contactPoints: this.config.contactPoints,
                localDataCenter: this.config.localDataCenter,
                keyspace: this.config.keyspace,
                credentials: {
                    username: this.config.username,
                    password: this.config.password
                },
                pooling: {
                    maxRequestsPerConnection: 32768,
                    coreConnectionsPerHost: {
                        [cassandra.types.distance.local]: 2,
                        [cassandra.types.distance.remote]: 1
                    }
                }
            });

            await this.client.connect();
            return this.client;
        } catch (error) {
            throw new DatabaseError(`Cassandra initialization failed: ${error.message}`);
        }
    }

    /**
     * Create keyspace if not exists
     * @param {string} keyspace - Keyspace name
     * @param {Object} options - Keyspace options
     */
    async createKeyspace(keyspace, options = {}) {
        try {
            const {
                replication = {
                    class: 'SimpleStrategy',
                    replication_factor: 1
                },
                durableWrites = true
            } = options;

            const query = `
                CREATE KEYSPACE IF NOT EXISTS ${keyspace}
                WITH REPLICATION = ${JSON.stringify(replication).replace(/"/g, "'")}
                AND DURABLE_WRITES = ${durableWrites}
            `;

            await this.client.execute(query);
            return true;
        } catch (error) {
            throw new DatabaseError(`Create keyspace failed: ${error.message}`);
        }
    }

    /**
     * Define a table model
     * @param {string} tableName - Table name
     * @param {Object} schema - Table schema
     * @param {Object} options - Table options
     */
    async defineModel(tableName, schema, options = {}) {
        try {
            const {
                partitionKey,
                clusteringKey = [],
                secondaryIndexes = [],
                withClusteringOrderBy = {}
            } = options;

            if (!partitionKey) {
                throw new Error('Partition key is required');
            }

            const columns = this._buildColumnDefinitions(schema);
            const primaryKey = this._buildPrimaryKey(partitionKey, clusteringKey);
            const indexes = this._buildSecondaryIndexes(tableName, secondaryIndexes);
            const clusteringOrder = this._buildClusteringOrder(withClusteringOrderBy);

            const query = `
                CREATE TABLE IF NOT EXISTS ${tableName} (
                    ${columns.join(',\n    ')},
                    PRIMARY KEY ${primaryKey}
                )
                ${clusteringOrder};
            `;

            await this.client.execute(query);

            // Create secondary indexes
            for (const indexQuery of indexes) {
                await this.client.execute(indexQuery);
            }

            // Prepare common statements
            await this._prepareStatements(tableName, schema, partitionKey, clusteringKey);

            this.models.set(tableName, {
                schema,
                partitionKey,
                clusteringKey,
                secondaryIndexes
            });

            return true;
        } catch (error) {
            throw new DatabaseError(`Define model failed: ${error.message}`);
        }
    }

    /**
     * Insert data
     * @param {string} tableName - Table name
     * @param {Object} data - Data to insert
     * @param {Object} options - Insert options
     */
    async insert(tableName, data, options = {}) {
        try {
            const {
                ttl,
                timestamp,
                consistency
            } = options;

            const model = this._getModel(tableName);
            const statement = this.preparedStatements.get(`${tableName}_insert`);
            const values = this._prepareValues(data, model.schema);

            const queryOptions = {
                prepare: true,
                consistency: consistency || cassandra.types.consistencies.quorum
            };

            if (ttl) queryOptions.ttl = ttl;
            if (timestamp) queryOptions.timestamp = timestamp;

            await this.client.execute(statement, values, queryOptions);
            return true;
        } catch (error) {
            throw new DatabaseError(`Insert failed: ${error.message}`);
        }
    }

    /**
     * Find data
     * @param {string} tableName - Table name
     * @param {Object} query - Query conditions
     * @param {Object} options - Query options
     */
    async find(tableName, query, options = {}) {
        try {
            const {
                select,
                limit,
                orderBy,
                consistency
            } = options;

            const model = this._getModel(tableName);
            const { cql, params } = this._buildSelectQuery(
                tableName,
                query,
                model,
                select,
                limit,
                orderBy
            );

            const queryOptions = {
                prepare: true,
                consistency: consistency || cassandra.types.consistencies.localQuorum
            };

            const result = await this.client.execute(cql, params, queryOptions);
            return result.rows.map(row => this._processRow(row, model.schema));
        } catch (error) {
            throw new DatabaseError(`Find failed: ${error.message}`);
        }
    }

    /**
     * Update data
     * @param {string} tableName - Table name
     * @param {Object} query - Query conditions
     * @param {Object} update - Update data
     * @param {Object} options - Update options
     */
    async update(tableName, query, update, options = {}) {
        try {
            const {
                ttl,
                timestamp,
                consistency
            } = options;

            const model = this._getModel(tableName);
            const { cql, params } = this._buildUpdateQuery(
                tableName,
                query,
                update,
                model,
                ttl
            );

            const queryOptions = {
                prepare: true,
                consistency: consistency || cassandra.types.consistencies.quorum
            };

            if (timestamp) queryOptions.timestamp = timestamp;

            await this.client.execute(cql, params, queryOptions);
            return true;
        } catch (error) {
            throw new DatabaseError(`Update failed: ${error.message}`);
        }
    }

    /**
     * Delete data
     * @param {string} tableName - Table name
     * @param {Object} query - Query conditions
     * @param {Object} options - Delete options
     */
    async delete(tableName, query, options = {}) {
        try {
            const {
                timestamp,
                consistency
            } = options;

            const model = this._getModel(tableName);
            const { cql, params } = this._buildDeleteQuery(tableName, query, model);

            const queryOptions = {
                prepare: true,
                consistency: consistency || cassandra.types.consistencies.quorum
            };

            if (timestamp) queryOptions.timestamp = timestamp;

            await this.client.execute(cql, params, queryOptions);
            return true;
        } catch (error) {
            throw new DatabaseError(`Delete failed: ${error.message}`);
        }
    }

    /**
     * Execute batch operations
     * @param {Array} operations - Array of operations
     * @param {Object} options - Batch options
     */
    async batch(operations, options = {}) {
        try {
            const {
                timestamp,
                consistency
            } = options;

            const queries = operations.map(op => {
                const model = this._getModel(op.table);
                switch (op.type) {
                    case 'insert':
                        return {
                            query: this.preparedStatements.get(`${op.table}_insert`),
                            params: this._prepareValues(op.data, model.schema)
                        };
                    case 'update':
                        return this._buildUpdateQuery(op.table, op.query, op.update, model);
                    case 'delete':
                        return this._buildDeleteQuery(op.table, op.query, model);
                    default:
                        throw new Error(`Unknown operation type: ${op.type}`);
                }
            });

            const queryOptions = {
                prepare: true,
                consistency: consistency || cassandra.types.consistencies.quorum
            };

            if (timestamp) queryOptions.timestamp = timestamp;

            await this.client.batch(queries, queryOptions);
            return true;
        } catch (error) {
            throw new DatabaseError(`Batch operation failed: ${error.message}`);
        }
    }

    /**
     * Stream large result sets
     * @param {string} tableName - Table name
     * @param {Object} query - Query conditions
     * @param {Object} options - Stream options
     */
    stream(tableName, query, options = {}) {
        const model = this._getModel(tableName);
        const { cql, params } = this._buildSelectQuery(
            tableName,
            query,
            model,
            options.select
        );

        const queryOptions = {
            prepare: true,
            fetchSize: options.fetchSize || 1000,
            autoPage: true
        };

        return this.client.stream(cql, params, queryOptions)
            .pipe(this._transformStream(model.schema));
    }

    /**
     * Close connection
     */
    async close() {
        try {
            await this.client.shutdown();
        } catch (error) {
            throw new DatabaseError(`Close failed: ${error.message}`);
        }
    }

    // Continuation of CassandraAdapter class private methods

    // Private helper methods
    _buildColumnDefinitions(schema) {
        return Object.entries(schema).map(([name, def]) => {
            return `${name} ${this._getCassandraType(def)}`;
        });
    }

    _getCassandraType(definition) {
        const typeMap = {
            'string': 'text',
            'number': 'double',
            'integer': 'int',
            'boolean': 'boolean',
            'date': 'timestamp',
            'uuid': 'uuid',
            'timeuuid': 'timeuuid',
            'map': 'map<text, text>',
            'list': 'list<text>',
            'set': 'set<text>',
            'json': 'text'
        };

        let type = typeMap[definition.type] || 'text';
        
        if (definition.type === 'map' && definition.keyType && definition.valueType) {
            type = `map<${typeMap[definition.keyType]}, ${typeMap[definition.valueType]}>`;
        }
        if (definition.type === 'list' && definition.itemType) {
            type = `list<${typeMap[definition.itemType]}>`;
        }
        if (definition.type === 'set' && definition.itemType) {
            type = `set<${typeMap[definition.itemType]}>`;
        }

        return type;
    }

    _buildPrimaryKey(partitionKey, clusteringKey) {
        const partition = Array.isArray(partitionKey) 
            ? `(${partitionKey.join(', ')})`
            : partitionKey;

        if (clusteringKey.length === 0) {
            return `(${partition})`;
        }

        return `(${partition}, ${clusteringKey.join(', ')})`;
    }

    _buildSecondaryIndexes(tableName, indexes) {
        return indexes.map(index => {
            const indexName = `${tableName}_${index}_idx`;
            return `CREATE INDEX IF NOT EXISTS ${indexName} ON ${tableName} (${index})`;
        });
    }

    _buildClusteringOrder(orderBy) {
        if (Object.keys(orderBy).length === 0) {
            return '';
        }

        const orders = Object.entries(orderBy)
            .map(([column, order]) => `${column} ${order.toUpperCase()}`)
            .join(', ');

        return `WITH CLUSTERING ORDER BY (${orders})`;
    }

    async _prepareStatements(tableName, schema, partitionKey, clusteringKey) {
        // Prepare insert statement
        const columns = Object.keys(schema);
        const placeholders = columns.map(() => '?').join(', ');
        const insertCql = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`;
        const insertStmt = await this.client.prepare(insertCql);
        this.preparedStatements.set(`${tableName}_insert`, insertStmt);

        // Prepare select by primary key statement
        const whereConditions = []
            .concat(Array.isArray(partitionKey) ? partitionKey : [partitionKey])
            .concat(clusteringKey)
            .map(key => `${key} = ?`)
            .join(' AND ');
        const selectCql = `SELECT * FROM ${tableName} WHERE ${whereConditions}`;
        const selectStmt = await this.client.prepare(selectCql);
        this.preparedStatements.set(`${tableName}_select_pk`, selectStmt);
    }

    _buildSelectQuery(tableName, query, model, select, limit, orderBy) {
        const columns = select ? select.join(', ') : '*';
        let cql = `SELECT ${columns} FROM ${tableName}`;
        const params = [];

        // Build WHERE clause
        const whereConditions = [];
        if (query) {
            for (const [key, value] of Object.entries(query)) {
                if (typeof value === 'object' && value !== null) {
                    for (const [op, val] of Object.entries(value)) {
                        const condition = this._buildOperatorCondition(key, op, val);
                        whereConditions.push(condition.clause);
                        params.push(...condition.params);
                    }
                } else {
                    whereConditions.push(`${key} = ?`);
                    params.push(value);
                }
            }
        }

        if (whereConditions.length > 0) {
            cql += ` WHERE ${whereConditions.join(' AND ')}`;
        }

        // Add ORDER BY
        if (orderBy) {
            const orderClauses = Object.entries(orderBy)
                .map(([column, order]) => `${column} ${order.toUpperCase()}`);
            cql += ` ORDER BY ${orderClauses.join(', ')}`;
        }

        // Add LIMIT
        if (limit) {
            cql += ' LIMIT ?';
            params.push(limit);
        }

        return { cql, params };
    }


    _buildUpdateQuery(tableName, query, update, model, ttl) {
        const sets = [];
        const params = [];

        // Build SET clause
        for (const [key, value] of Object.entries(update)) {
            if (key !== model.partitionKey && !model.clusteringKey.includes(key)) {
                if (typeof value === 'object' && value !== null) {
                    // Handle collections
                    if (value.$add) {
                        sets.push(`${key} = ${key} + ?`);
                        params.push(this._convertValue(value.$add, model.schema[key]));
                    } else if (value.$remove) {
                        sets.push(`${key} = ${key} - ?`);
                        params.push(this._convertValue(value.$remove, model.schema[key]));
                    } else if (value.$set) {
                        sets.push(`${key} = ?`);
                        params.push(this._convertValue(value.$set, model.schema[key]));
                    }
                } else {
                    sets.push(`${key} = ?`);
                    params.push(this._convertValue(value, model.schema[key]));
                }
            }
        }

        // Build WHERE clause
        const whereConditions = [];
        for (const [key, value] of Object.entries(query)) {
            whereConditions.push(`${key} = ?`);
            params.push(this._convertValue(value, model.schema[key]));
        }

        let cql = `UPDATE ${tableName}`;
        if (ttl) {
            cql += ` USING TTL ${ttl}`;
        }
        cql += ` SET ${sets.join(', ')} WHERE ${whereConditions.join(' AND ')}`;

        return { cql, params };
    }

    _buildDeleteQuery(tableName, query, model) {
        const whereConditions = [];
        const params = [];

        for (const [key, value] of Object.entries(query)) {
            whereConditions.push(`${key} = ?`);
            params.push(this._convertValue(value, model.schema[key]));
        }

        const cql = `DELETE FROM ${tableName} WHERE ${whereConditions.join(' AND ')}`;
        return { cql, params };
    }

    _buildOperatorCondition(key, operator, value) {
        const operatorMap = {
            $eq: '=',
            $gt: '>',
            $gte: '>=',
            $lt: '<',
            $lte: '<=',
            $in: 'IN'
        };

        const op = operatorMap[operator];
        if (!op) {
            throw new Error(`Unsupported operator: ${operator}`);
        }

        if (operator === '$in') {
            return {
                clause: `${key} IN ?`,
                params: [value]
            };
        }

        return {
            clause: `${key} ${op} ?`,
            params: [value]
        };
    }

    _convertValue(value, definition) {
        if (value === null || value === undefined) {
            return null;
        }

        switch (definition.type) {
            case 'uuid':
            case 'timeuuid':
                return value.toString();
            case 'date':
                return new Date(value);
            case 'json':
                return typeof value === 'string' ? value : JSON.stringify(value);
            case 'map':
            case 'list':
            case 'set':
                return value;
            default:
                return value;
        }
    }

    _processRow(row, schema) {
        const processed = {};
        for (const [key, value] of Object.entries(row)) {
            if (value !== null && value !== undefined) {
                processed[key] = this._processValue(value, schema[key]);
            }
        }
        return processed;
    }

    _processValue(value, definition) {
        if (value === null || value === undefined) {
            return null;
        }

        switch (definition.type) {
            case 'json':
                try {
                    return JSON.parse(value);
                } catch {
                    return value;
                }
            case 'date':
                return value instanceof Date ? value : new Date(value);
            default:
                return value;
        }
    }

    _transformStream(schema) {
        const Transform = require('stream').Transform;
        return new Transform({
            objectMode: true,
            transform: (row, encoding, callback) => {
                try {
                    callback(null, this._processRow(row, schema));
                } catch (error) {
                    callback(error);
                }
            }
        });
    }

    _getModel(tableName) {
        const model = this.models.get(tableName);
        if (!model) {
            throw new Error(`Model ${tableName} not found`);
        }
        return model;
    }
}

module.exports = CassandraAdapter;