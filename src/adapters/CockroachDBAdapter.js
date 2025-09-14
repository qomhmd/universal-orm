
const { Pool } = require('pg');
const BaseAdapter = require('./BaseAdapter');
const { DatabaseError } = require('../utils/errors');

class CockroachDBAdapter extends BaseAdapter {
    constructor(config) {
        super();
        this.config = config;
        this.pool = null;
        this.models = new Map();
    }

    /**
     * Initialize CockroachDB connection
     */
    async initialize() {
        try {
            this.pool = new Pool({
                host: this.config.host,
                port: this.config.port,
                database: this.config.database,
                user: this.config.user,
                password: this.config.password,
                ssl: this.config.ssl || false,
                max: this.config.poolSize || 20,
                idleTimeoutMillis: 30000,
                connectionTimeoutMillis: 2000,
                application_name: 'universal-orm'
            });

            // Test connection
            await this.pool.query('SELECT version()');
            return this.pool;
        } catch (error) {
            throw new DatabaseError(`CockroachDB initialization failed: ${error.message}`);
        }
    }

    /**
     * Create table with schema
     * @param {string} tableName - Table name
     * @param {Object} schema - Table schema
     * @param {Object} options - Table options
     */
    async createTable(tableName, schema, options = {}) {
        try {
            const {
                primaryKey = 'id',
                indexes = [],
                partitionBy = null,
                interleaveIn = null
            } = options;

            // Build column definitions
            const columns = Object.entries(schema).map(([name, def]) => 
                this._buildColumnDefinition(name, def)
            );

            // Add primary key if not in schema
            if (!schema[primaryKey]) {
                columns.unshift(`${primaryKey} UUID PRIMARY KEY DEFAULT gen_random_uuid()`);
            }

            let query = `
                CREATE TABLE IF NOT EXISTS ${tableName} (
                    ${columns.join(',\n    ')}
                )
            `;

            // Add partitioning
            if (partitionBy) {
                query += ` PARTITION BY ${partitionBy}`;
            }

            // Add interleaving
            if (interleaveIn) {
                query += ` INTERLEAVE IN PARENT ${interleaveIn.table} (${interleaveIn.columns.join(', ')})`;
            }

            await this.pool.query(query);

            // Create indexes
            for (const index of indexes) {
                await this.createIndex(tableName, index);
            }

            this.models.set(tableName, { schema, primaryKey });
            return true;
        } catch (error) {
            throw new DatabaseError(`Create table failed: ${error.message}`);
        }
    }

    /**
     * Create index
     * @param {string} tableName - Table name
     * @param {Object} index - Index definition
     */
    async createIndex(tableName, index) {
        try {
            const {
                name,
                columns,
                unique = false,
                type = null,
                where = null
            } = index;

            let query = `CREATE ${unique ? 'UNIQUE' : ''} INDEX `;
            if (name) {
                query += `${name} `;
            }
            query += `ON ${tableName}`;
            
            if (type) {
                query += ` USING ${type}`;
            }

            query += ` (${Array.isArray(columns) ? columns.join(', ') : columns})`;

            if (where) {
                query += ` WHERE ${where}`;
            }

            await this.pool.query(query);
            return true;
        } catch (error) {
            throw new DatabaseError(`Create index failed: ${error.message}`);
        }
    }

    /**
     * Insert records
     * @param {string} tableName - Table name
     * @param {Object|Array} data - Data to insert
     * @param {Object} options - Insert options
     */
    async insert(tableName, data, options = {}) {
        const client = await this.pool.connect();
        try {
            const {
                returning = '*',
                onConflict = null
            } = options;

            const records = Array.isArray(data) ? data : [data];
            if (records.length === 0) return [];

            const model = this._getModel(tableName);
            const columns = Object.keys(records[0]);
            const values = records.map(record => 
                columns.map(col => this._formatValue(record[col], model.schema[col]))
            );

            let query = `
                INSERT INTO ${tableName} (${columns.join(', ')})
                VALUES ${this._buildValuePlaceholders(records.length, columns.length)}
            `;

            if (onConflict) {
                query += ` ON CONFLICT ${onConflict}`;
            }

            if (returning) {
                query += ` RETURNING ${returning}`;
            }

            const result = await client.query(query, values.flat());
            return result.rows;
        } catch (error) {
            throw new DatabaseError(`Insert failed: ${error.message}`);
        } finally {
            client.release();
        }
    }

    /**
     * Find records
     * @param {string} tableName - Table name
     * @param {Object} query - Query conditions
     * @param {Object} options - Query options
     */
    async find(tableName, query = {}, options = {}) {
        const client = await this.pool.connect();
        try {
            const {
                select = '*',
                orderBy,
                limit,
                offset,
                forUpdate = false
            } = options;

            const { text, values } = this._buildSelectQuery(
                tableName,
                query,
                select,
                orderBy,
                limit,
                offset,
                forUpdate
            );

            const result = await client.query(text, values);
            return result.rows;
        } catch (error) {
            throw new DatabaseError(`Find failed: ${error.message}`);
        } finally {
            client.release();
        }
    }

    /**
     * Update records
     * @param {string} tableName - Table name
     * @param {Object} query - Query conditions
     * @param {Object} update - Update data
     * @param {Object} options - Update options
     */
    async update(tableName, query, update, options = {}) {
        const client = await this.pool.connect();
        try {
            const {
                returning = '*'
            } = options;

            const model = this._getModel(tableName);
            const { conditions, values } = this._buildWhereClause(query);
            const setClause = this._buildSetClause(update, model.schema);

            const queryText = `
                UPDATE ${tableName}
                SET ${setClause.text}
                ${conditions ? `WHERE ${conditions}` : ''}
                ${returning ? `RETURNING ${returning}` : ''}
            `;

            const result = await client.query(queryText, [...setClause.values, ...values]);
            return result.rows;
        } catch (error) {
            throw new DatabaseError(`Update failed: ${error.message}`);
        } finally {
            client.release();
        }
    }

    /**
     * Delete records
     * @param {string} tableName - Table name
     * @param {Object} query - Query conditions
     * @param {Object} options - Delete options
     */
    async delete(tableName, query, options = {}) {
        const client = await this.pool.connect();
        try {
            const {
                returning = '*'
            } = options;

            const { conditions, values } = this._buildWhereClause(query);

            const queryText = `
                DELETE FROM ${tableName}
                ${conditions ? `WHERE ${conditions}` : ''}
                ${returning ? `RETURNING ${returning}` : ''}
            `;

            const result = await client.query(queryText, values);
            return result.rows;
        } catch (error) {
            throw new DatabaseError(`Delete failed: ${error.message}`);
        } finally {
            client.release();
        }
    }

    /**
     * Execute transaction
     * @param {Function} callback - Transaction callback
     * @param {Object} options - Transaction options
     */
    async transaction(callback, options = {}) {
        const client = await this.pool.connect();
        try {
            await client.query('BEGIN');
            
            if (options.priority === 'high') {
                await client.query('SET TRANSACTION PRIORITY HIGH');
            }

            const result = await callback(client);
            await client.query('COMMIT');
            return result;
        } catch (error) {
            await client.query('ROLLBACK');
            throw new DatabaseError(`Transaction failed: ${error.message}`);
        } finally {
            client.release();
        }
    }

    /**
     * Execute raw query
     * @param {string} query - SQL query
     * @param {Array} values - Query values
     */
    async query(query, values = []) {
        const client = await this.pool.connect();
        try {
            const result = await client.query(query, values);
            return result.rows;
        } catch (error) {
            throw new DatabaseError(`Query failed: ${error.message}`);
        } finally {
            client.release();
        }
    }

    /**
     * Close connection pool
     */
    async close() {
        try {
            await this.pool.end();
        } catch (error) {
            throw new DatabaseError(`Close failed: ${error.message}`);
        }
    }

   // Continuation of CockroachDBAdapter class private methods

    /**
     * Build column definition
     * @private
     */
    _buildColumnDefinition(name, definition) {
        let columnDef = `${name} ${this._getColumnType(definition)}`;

        if (definition.primaryKey) {
            columnDef += ' PRIMARY KEY';
        }
        if (definition.unique) {
            columnDef += ' UNIQUE';
        }
        if (definition.notNull) {
            columnDef += ' NOT NULL';
        }
        if (definition.default !== undefined) {
            columnDef += ` DEFAULT ${this._formatDefaultValue(definition.default, definition.type)}`;
        }
        if (definition.check) {
            columnDef += ` CHECK (${definition.check})`;
        }
        if (definition.references) {
            columnDef += ` REFERENCES ${definition.references}`;
        }

        return columnDef;
    }

    /**
     * Get column type
     * @private
     */
    _getColumnType(definition) {
        const typeMap = {
            'string': 'VARCHAR',
            'text': 'TEXT',
            'integer': 'INT8',
            'float': 'FLOAT8',
            'boolean': 'BOOLEAN',
            'date': 'TIMESTAMP',
            'uuid': 'UUID',
            'json': 'JSONB',
            'array': 'ARRAY',
            'decimal': 'DECIMAL'
        };

        let type = typeMap[definition.type] || 'VARCHAR';

        if (definition.type === 'array') {
            type = `${typeMap[definition.itemType] || 'VARCHAR'}[]`;
        }
        if (definition.type === 'decimal' && definition.precision) {
            type = `DECIMAL(${definition.precision}${definition.scale ? `,${definition.scale}` : ''})`;
        }
        if (definition.length) {
            type = `${type}(${definition.length})`;
        }

        return type;
    }

    /**
     * Build SELECT query
     * @private
     */
    _buildSelectQuery(tableName, query, select, orderBy, limit, offset, forUpdate) {
        const { conditions, values } = this._buildWhereClause(query);

        let queryText = `
            SELECT ${select}
            FROM ${tableName}
            ${conditions ? `WHERE ${conditions}` : ''}
        `;

        if (orderBy) {
            const orderClauses = Object.entries(orderBy)
                .map(([column, order]) => `${column} ${order === -1 ? 'DESC' : 'ASC'}`);
            queryText += ` ORDER BY ${orderClauses.join(', ')}`;
        }

        if (limit) {
            queryText += ` LIMIT ${limit}`;
        }
        if (offset) {
            queryText += ` OFFSET ${offset}`;
        }
        if (forUpdate) {
            queryText += ' FOR UPDATE';
        }

        return { text: queryText, values };
    }

    /**
     * Build WHERE clause
     * @private
     */
    _buildWhereClause(query) {
        const conditions = [];
        const values = [];
        let paramCount = 1;

        for (const [key, value] of Object.entries(query)) {
            if (typeof value === 'object' && value !== null) {
                for (const [operator, operand] of Object.entries(value)) {
                    const { condition, value: operandValue } = this._buildOperatorCondition(
                        key,
                        operator,
                        operand,
                        paramCount
                    );
                    conditions.push(condition);
                    values.push(operandValue);
                    paramCount++;
                }
            } else {
                conditions.push(`${key} = $${paramCount}`);
                values.push(value);
                paramCount++;
            }
        }

        return {
            conditions: conditions.length > 0 ? conditions.join(' AND ') : '',
            values
        };
    }

    /**
     * Build operator condition
     * @private
     */
    _buildOperatorCondition(key, operator, value, paramCount) {
        switch (operator) {
            case '$eq':
                return { condition: `${key} = $${paramCount}`, value };
            case '$ne':
                return { condition: `${key} != $${paramCount}`, value };
            case '$gt':
                return { condition: `${key} > $${paramCount}`, value };
            case '$gte':
                return { condition: `${key} >= $${paramCount}`, value };
            case '$lt':
                return { condition: `${key} < $${paramCount}`, value };
            case '$lte':
                return { condition: `${key} <= $${paramCount}`, value };
            case '$in':
                return { condition: `${key} = ANY($${paramCount})`, value };
            case '$nin':
                return { condition: `${key} != ALL($${paramCount})`, value };
            case '$like':
                return { condition: `${key} LIKE $${paramCount}`, value };
            case '$ilike':
                return { condition: `${key} ILIKE $${paramCount}`, value };
            case '$contains':
                return { condition: `${key} @> $${paramCount}`, value };
            case '$contained':
                return { condition: `${key} <@ $${paramCount}`, value };
            case '$overlap':
                return { condition: `${key} && $${paramCount}`, value };
            case '$any':
                return { condition: `$${paramCount} = ANY(${key})`, value };
            case '$all':
                return { condition: `$${paramCount} = ALL(${key})`, value };
            default:
                throw new Error(`Unknown operator: ${operator}`);
        }
    }

    /**
     * Build SET clause for UPDATE
     * @private
     */
    _buildSetClause(update, schema) {
        const setClauses = [];
        const values = [];
        let paramCount = 1;

        for (const [key, value] of Object.entries(update)) {
            if (typeof value === 'object' && value !== null) {
                if (value.$inc) {
                    setClauses.push(`${key} = ${key} + $${paramCount}`);
                    values.push(value.$inc);
                    paramCount++;
                } else if (value.$set) {
                    setClauses.push(`${key} = $${paramCount}`);
                    values.push(value.$set);
                    paramCount++;
                } else if (value.$push) {
                    setClauses.push(`${key} = array_append(${key}, $${paramCount})`);
                    values.push(value.$push);
                    paramCount++;
                } else if (value.$pull) {
                    setClauses.push(`${key} = array_remove(${key}, $${paramCount})`);
                    values.push(value.$pull);
                    paramCount++;
                } else if (value.$unset) {
                    setClauses.push(`${key} = NULL`);
                }
            } else {
                setClauses.push(`${key} = $${paramCount}`);
                values.push(this._formatValue(value, schema[key]));
                paramCount++;
            }
        }

        return {
            text: setClauses.join(', '),
            values
        };
    }

    /**
     * Build value placeholders for INSERT
     * @private
     */
    _buildValuePlaceholders(rowCount, columnCount) {
        const rows = [];
        let paramCount = 1;

        for (let i = 0; i < rowCount; i++) {
            const placeholders = [];
            for (let j = 0; j < columnCount; j++) {
                placeholders.push(`$${paramCount}`);
                paramCount++;
            }
            rows.push(`(${placeholders.join(', ')})`);
        }

        return rows.join(', ');
    }

    /**
     * Format value based on type
     * @private
     */
    _formatValue(value, definition) {
        if (value === null || value === undefined) {
            return null;
        }

        switch (definition.type) {
            case 'json':
                return typeof value === 'string' ? value : JSON.stringify(value);
            case 'date':
                return value instanceof Date ? value : new Date(value);
            case 'array':
                return Array.isArray(value) ? value : [value];
            case 'decimal':
                return typeof value === 'string' ? value : value.toString();
            default:
                return value;
        }
    }

    /**
     * Format default value
     * @private
     */
    _formatDefaultValue(value, type) {
        if (typeof value === 'function') {
            return value();
        }

        switch (type) {
            case 'string':
            case 'text':
                return `'${value}'`;
            case 'json':
                return `'${JSON.stringify(value)}'::jsonb`;
            case 'array':
                return `ARRAY[${value.map(v => `'${v}'`).join(', ')}]`;
            case 'date':
                return value === 'now' ? 'CURRENT_TIMESTAMP' : `'${value}'`;
            default:
                return value;
        }
    }

    /**
     * Get model
     * @private
     */
    _getModel(tableName) {
        const model = this.models.get(tableName);
        if (!model) {
            throw new Error(`Model ${tableName} not found`);
        }
        return model;
    }
}

module.exports = CockroachDBAdapter;