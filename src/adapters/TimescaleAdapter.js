
const { Pool } = require('pg');
const { BaseAdapter } = require('./BaseAdapter');
const { DatabaseError } = require('../utils/errors');
const PostgresAdapter = require('./PostgresAdapter');

class TimescaleAdapter extends PostgresAdapter {
    constructor(config) {
        super(config);
        this.hypertables = new Map();
    }

    /**
     * Create a hypertable for time-series data
     * @param {string} tableName - Name of the table
     * @param {Object} schema - Table schema
     * @param {Object} options - Hypertable options
     */
    async createHypertable(tableName, schema, options = {}) {
        try {
            const {
                timeColumn = 'time',
                chunkTimeInterval = '7 days',
                compressionEnabled = true,
                retentionPolicy = null,
                spacePartitions = null
            } = options;

            // Create regular table first
            const createTableQuery = this._buildCreateTableQuery(tableName, schema);
            await this.query(createTableQuery);

            // Convert to hypertable
            await this.query(`
                SELECT create_hypertable(
                    '${tableName}',
                    '${timeColumn}',
                    ${spacePartitions ? `partitioning_column => '${spacePartitions}'` : 'create_default_indexes => TRUE'},
                    chunk_time_interval => INTERVAL '${chunkTimeInterval}'
                );
            `);

            // Enable compression if requested
            if (compressionEnabled) {
                await this._enableCompression(tableName, timeColumn);
            }

            // Set retention policy if specified
            if (retentionPolicy) {
                await this._setRetentionPolicy(tableName, retentionPolicy);
            }

            this.hypertables.set(tableName, {
                schema,
                timeColumn,
                chunkTimeInterval,
                compressionEnabled,
                retentionPolicy
            });

            return true;
        } catch (error) {
            throw new DatabaseError(`Failed to create hypertable: ${error.message}`);
        }
    }

    /**
     * Insert time-series data
     * @param {string} tableName - Hypertable name
     * @param {Array} data - Array of time-series data points
     */
    async insertTimeSeriesData(tableName, data) {
        try {
            const hypertable = this._getHypertable(tableName);
            const columns = Object.keys(hypertable.schema);
            
            const values = data.map(point => {
                return `(${columns.map(col => this._formatValue(point[col], hypertable.schema[col].type)).join(', ')})`;
            }).join(', ');

            const query = `
                INSERT INTO ${tableName} (${columns.join(', ')})
                VALUES ${values}
            `;

            return await this.query(query);
        } catch (error) {
            throw new DatabaseError(`Failed to insert time-series data: ${error.message}`);
        }
    }

    /**
     * Query time-series data with time-based aggregations
     * @param {string} tableName - Hypertable name
     * @param {Object} options - Query options
     */
    async queryTimeSeriesData(tableName, options) {
        try {
            const {
                timeColumn,
                startTime,
                endTime,
                interval,
                aggregates,
                groupBy = [],
                where = {},
                orderBy = 'time',
                limit
            } = options;

            const hypertable = this._getHypertable(tableName);
            const whereClause = this._buildWhereClause(where);
            const timeWhereClause = this._buildTimeWhereClause(timeColumn || hypertable.timeColumn, startTime, endTime);
            const aggregateColumns = this._buildAggregateColumns(aggregates);
            const groupByClause = this._buildGroupByClause(interval, groupBy);
            const orderByClause = this._buildOrderByClause(orderBy);
            const limitClause = limit ? `LIMIT ${limit}` : '';

            const query = `
                SELECT
                    ${interval ? `time_bucket('${interval}', ${timeColumn || hypertable.timeColumn}) as bucket,` : ''}
                    ${aggregateColumns}
                FROM ${tableName}
                WHERE ${timeWhereClause}
                ${whereClause ? `AND ${whereClause}` : ''}
                ${groupByClause}
                ${orderByClause}
                ${limitClause}
            `;

            return await this.query(query);
        } catch (error) {
            throw new DatabaseError(`Failed to query time-series data: ${error.message}`);
        }
    }

    /**
     * Continuous aggregates for real-time materialized views
     * @param {string} viewName - Name of the continuous aggregate view
     * @param {string} hypertable - Source hypertable
     * @param {Object} options - Aggregation options
     */
    async createContinuousAggregate(viewName, hypertable, options) {
        try {
            const {
                timeColumn,
                interval,
                aggregates,
                groupBy = [],
                withData = true
            } = options;

            const aggregateColumns = this._buildAggregateColumns(aggregates);
            const groupByColumns = groupBy.join(', ');

            const query = `
                CREATE MATERIALIZED VIEW ${viewName}
                WITH (timescaledb.continuous) AS
                SELECT
                    time_bucket('${interval}', ${timeColumn}) as bucket,
                    ${aggregateColumns}
                FROM ${hypertable}
                GROUP BY bucket${groupByColumns ? `, ${groupByColumns}` : ''}
                WITH ${withData ? 'DATA' : 'NO DATA'}
            `;

            return await this.query(query);
        } catch (error) {
            throw new DatabaseError(`Failed to create continuous aggregate: ${error.message}`);
        }
    }

    /**
     * Add compression policy to a hypertable
     * @param {string} tableName - Hypertable name
     * @param {Object} options - Compression options
     */
    async addCompressionPolicy(tableName, options) {
        try {
            const {
                compress_after = '7 days',
                compress_columns = '*'
            } = options;

            await this.query(`
                ALTER TABLE ${tableName} SET (
                    timescaledb.compress = true,
                    timescaledb.compress_segmentby = '',
                    timescaledb.compress_orderby = 'time'
                );

                SELECT add_compression_policy('${tableName}', 
                    INTERVAL '${compress_after}');
            `);

            return true;
        } catch (error) {
            throw new DatabaseError(`Failed to add compression policy: ${error.message}`);
        }
    }

    /**
     * Get time-series statistics
     * @param {string} tableName - Hypertable name
     */
    async getTimeSeriesStats(tableName) {
        try {
            const stats = await this.query(`
                SELECT
                    hypertable_schema,
                    hypertable_name,
                    total_chunks,
                    compressed_total_bytes,
                    uncompressed_total_bytes,
                    compression_ratio
                FROM timescaledb_information.hypertable_compression_stats
                WHERE hypertable_name = '${tableName}';
            `);

            const size = await this.query(`
                SELECT hypertable_size('${tableName}') as size;
            `);

            return {
                ...stats[0],
                total_size_bytes: size[0].size
            };
        } catch (error) {
            throw new DatabaseError(`Failed to get time-series stats: ${error.message}`);
        }
    }

    // Private helper methods
    async _enableCompression(tableName, timeColumn) {
        await this.query(`
            ALTER TABLE ${tableName} SET (
                timescaledb.compress = true,
                timescaledb.compress_orderby = '${timeColumn} DESC'
            );
        `);
    }

    async _setRetentionPolicy(tableName, retention) {
        await this.query(`
            SELECT add_retention_policy('${tableName}', 
                INTERVAL '${retention}');
        `);
    }

    _buildCreateTableQuery(tableName, schema) {
        const columns = Object.entries(schema).map(([name, def]) => {
            return `${name} ${this._getColumnType(def)}`;
        }).join(', ');

        return `CREATE TABLE ${tableName} (${columns});`;
    }

    _getColumnType(definition) {
        const typeMap = {
            'timestamp': 'TIMESTAMPTZ',
            'double': 'DOUBLE PRECISION',
            'integer': 'INTEGER',
            'string': 'TEXT',
            'boolean': 'BOOLEAN',
            'json': 'JSONB'
        };

        return typeMap[definition.type] || 'TEXT';
    }

    _buildTimeWhereClause(timeColumn, startTime, endTime) {
        const conditions = [];
        
        if (startTime) {
            conditions.push(`${timeColumn} >= '${startTime}'`);
        }
        if (endTime) {
            conditions.push(`${timeColumn} <= '${endTime}'`);
        }

        return conditions.length > 0 ? conditions.join(' AND ') : '1=1';
    }

    _buildAggregateColumns(aggregates = {}) {
        return Object.entries(aggregates)
            .map(([alias, agg]) => {
                const { function: func, column } = agg;
                return `${func}(${column}) as ${alias}`;
            })
            .join(', ');
    }

    _buildGroupByClause(interval, groupBy) {
        const groups = [];
        if (interval) groups.push('bucket');
        if (groupBy.length > 0) groups.push(...groupBy);
        
        return groups.length > 0 ? `GROUP BY ${groups.join(', ')}` : '';
    }

    _buildOrderByClause(orderBy) {
        return orderBy ? `ORDER BY ${orderBy}` : '';
    }

    _formatValue(value, type) {
        if (value === null) return 'NULL';
        
        switch (type) {
            case 'timestamp':
                return `'${value}'`;
            case 'string':
                return `'${value.replace(/'/g, "''")}'`;
            case 'json':
                return `'${JSON.stringify(value)}'::jsonb`;
            default:
                return value;
        }
    }

    _getHypertable(tableName) {
        const hypertable = this.hypertables.get(tableName);
        if (!hypertable) {
            throw new Error(`Hypertable ${tableName} not found`);
        }
        return hypertable;
    }
}

module.exports = TimescaleAdapter;