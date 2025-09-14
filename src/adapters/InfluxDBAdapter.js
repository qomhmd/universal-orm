// src/adapters/InfluxDBAdapter.js
const { InfluxDB, Point, HttpError } = require('@influxdata/influxdb-client');
const { BaseAdapter } = require('./BaseAdapter');
const { DatabaseError } = require('../utils/errors');

class InfluxDBAdapter extends BaseAdapter {
    constructor(config) {
        super();
        this.config = config;
        this.client = null;
        this.writeApi = null;
        this.queryApi = null;
    }

    /**
     * Initialize InfluxDB connection
     */
    async initialize() {
        try {
            this.client = new InfluxDB({
                url: this.config.url,
                token: this.config.token,
                timeout: this.config.timeout || 10000
            });

            this.writeApi = this.client.getWriteApi(
                this.config.org,
                this.config.bucket,
                this.config.precision || 'ns'
            );

            this.queryApi = this.client.getQueryApi(this.config.org);

            // Verify connection
            await this._ping();
            return this.client;
        } catch (error) {
            throw new DatabaseError(`InfluxDB initialization failed: ${error.message}`);
        }
    }

    /**
     * Write a single point
     * @param {string} measurement - Measurement name
     * @param {Object} fields - Field key-value pairs
     * @param {Object} tags - Tag key-value pairs
     * @param {Date} timestamp - Timestamp
     */
    async writePoint(measurement, fields, tags = {}, timestamp = new Date()) {
        try {
            const point = new Point(measurement)
                .timestamp(timestamp);

            // Add fields
            for (const [key, value] of Object.entries(fields)) {
                switch (typeof value) {
                    case 'number':
                        point.floatField(key, value);
                        break;
                    case 'boolean':
                        point.booleanField(key, value);
                        break;
                    case 'string':
                        point.stringField(key, value);
                        break;
                    default:
                        point.stringField(key, JSON.stringify(value));
                }
            }

            // Add tags
            for (const [key, value] of Object.entries(tags)) {
                point.tag(key, String(value));
            }

            await this.writeApi.writePoint(point);
            await this.writeApi.flush();
            return true;
        } catch (error) {
            throw new DatabaseError(`Write point failed: ${error.message}`);
        }
    }

    /**
     * Write multiple points
     * @param {Array} points - Array of points to write
     */
    async writePoints(points) {
        try {
            const influxPoints = points.map(({ measurement, fields, tags, timestamp }) => {
                const point = new Point(measurement)
                    .timestamp(timestamp || new Date());

                // Add fields
                for (const [key, value] of Object.entries(fields)) {
                    switch (typeof value) {
                        case 'number':
                            point.floatField(key, value);
                            break;
                        case 'boolean':
                            point.booleanField(key, value);
                            break;
                        case 'string':
                            point.stringField(key, value);
                            break;
                        default:
                            point.stringField(key, JSON.stringify(value));
                    }
                }

                // Add tags
                if (tags) {
                    for (const [key, value] of Object.entries(tags)) {
                        point.tag(key, String(value));
                    }
                }

                return point;
            });

            await this.writeApi.writePoints(influxPoints);
            await this.writeApi.flush();
            return true;
        } catch (error) {
            throw new DatabaseError(`Write points failed: ${error.message}`);
        }
    }

    /**
     * Query data using Flux
     * @param {string} query - Flux query
     * @param {Object} params - Query parameters
     */
    async query(query, params = {}) {
        try {
            const rows = await this._collectRows(query, params);
            return this._processQueryResults(rows);
        } catch (error) {
            throw new DatabaseError(`Query failed: ${error.message}`);
        }
    }

    /**
     * Query data with builder pattern
     * @param {Object} options - Query options
     */
    async queryBuilder(options) {
        try {
            const {
                measurement,
                fields,
                tags,
                range,
                aggregateWindow,
                filters,
                groupBy
            } = options;

            let query = `
                from(bucket: "${this.config.bucket}")
                |> range(${this._buildRangeExpression(range)})
            `;

            if (measurement) {
                query += `\n    |> filter(fn: (r) => r["_measurement"] == "${measurement}")`;
            }

            if (fields) {
                const fieldList = Array.isArray(fields) ? fields : [fields];
                query += `\n    |> filter(fn: (r) => ${fieldList.map(f => `r["_field"] == "${f}"`).join(' or ')})`;
            }

            if (tags) {
                for (const [key, value] of Object.entries(tags)) {
                    if (Array.isArray(value)) {
                        query += `\n    |> filter(fn: (r) => ${value.map(v => `r["${key}"] == "${v}"`).join(' or ')})`;
                    } else {
                        query += `\n    |> filter(fn: (r) => r["${key}"] == "${value}")`;
                    }
                }
            }

            if (filters) {
                query += `\n    |> filter(fn: (r) => ${this._buildFilterExpression(filters)})`;
            }

            if (aggregateWindow) {
                const { every, fn } = aggregateWindow;
                query += `\n    |> aggregateWindow(every: ${every}, fn: ${fn}, createEmpty: false)`;
            }

            if (groupBy) {
                const columns = Array.isArray(groupBy) ? groupBy : [groupBy];
                query += `\n    |> group(columns: [${columns.map(c => `"${c}"`).join(', ')}])`;
            }

            const rows = await this._collectRows(query);
            return this._processQueryResults(rows);
        } catch (error) {
            throw new DatabaseError(`Query builder failed: ${error.message}`);
        }
    }

    /**
     * Delete data
     * @param {Object} options - Delete options
     */
    async delete(options) {
        try {
            const {
                measurement,
                tags,
                range
            } = options;

            let query = `
                import "influxdata/influxdb/tasks"
                import "influxdata/influxdb/v1"

                from(bucket: "${this.config.bucket}")
                    |> range(${this._buildRangeExpression(range)})
            `;

            if (measurement) {
                query += `\n    |> filter(fn: (r) => r["_measurement"] == "${measurement}")`;
            }

            if (tags) {
                for (const [key, value] of Object.entries(tags)) {
                    query += `\n    |> filter(fn: (r) => r["${key}"] == "${value}")`;
                }
            }

            query += '\n    |> v1.delete()';

            await this.queryApi.queryRaw(query);
            return true;
        } catch (error) {
            throw new DatabaseError(`Delete failed: ${error.message}`);
        }
    }

    /**
     * Create a retention policy
     * @param {Object} options - Retention policy options
     */
    async createRetentionPolicy(options) {
        try {
            const {
                name,
                duration,
                replication = 1,
                isDefault = false
            } = options;

            let query = `
                import "influxdata/influxdb/v1"

                v1.retention_policy(
                    name: "${name}",
                    bucket_id: "${this.config.bucket}",
                    duration: ${duration},
                    replication: ${replication}
                    ${isDefault ? ', default: true' : ''}
                )
            `;

            await this.queryApi.queryRaw(query);
            return true;
        } catch (error) {
            throw new DatabaseError(`Create retention policy failed: ${error.message}`);
        }
    }

    /**
     * Close connection
     */
    async close() {
        try {
            await this.writeApi.close();
            this.client = null;
        } catch (error) {
            throw new DatabaseError(`Close failed: ${error.message}`);
        }
    }

    // Private helper methods
    async _ping() {
        try {
            await this.client.ping();
        } catch (error) {
            throw new Error('InfluxDB connection failed');
        }
    }

    async _collectRows(query, params = {}) {
        const rows = [];
        for await (const row of this.queryApi.queryRows(query, params)) {
            rows.push(row);
        }
        return rows;
    }

    _processQueryResults(rows) {
        const results = [];
        
        for (const row of rows) {
            const result = {
                timestamp: row._time,
                measurement: row._measurement,
                field: row._field,
                value: row._value
            };

            // Add tags
            for (const [key, value] of Object.entries(row)) {
                if (!key.startsWith('_') && value !== undefined) {
                    result[key] = value;
                }
            }

            results.push(result);
        }

        return results;
    }

    _buildRangeExpression(range) {
        if (typeof range === 'string') {
            return `start: ${range}`;
        }

        const { start, stop } = range;
        let expression = '';

        if (start) {
            expression += `start: ${start}`;
        }
        if (stop) {
            expression += `${expression ? ', ' : ''}stop: ${stop}`;
        }

        return expression;
    }

    _buildFilterExpression(filters) {
        const expressions = [];

        for (const [field, conditions] of Object.entries(filters)) {
            if (typeof conditions === 'object') {
                for (const [operator, value] of Object.entries(conditions)) {
                    expressions.push(this._buildOperatorExpression(field, operator, value));
                }
            } else {
                expressions.push(`r["${field}"] == ${JSON.stringify(conditions)}`);
            }
        }

        return expressions.join(' and ');
    }

    _buildOperatorExpression(field, operator, value) {
        switch (operator) {
            case '$eq':
                return `r["${field}"] == ${JSON.stringify(value)}`;
            case '$ne':
                return `r["${field}"] != ${JSON.stringify(value)}`;
            case '$gt':
                return `r["${field}"] > ${value}`;
            case '$gte':
                return `r["${field}"] >= ${value}`;
            case '$lt':
                return `r["${field}"] < ${value}`;
            case '$lte':
                return `r["${field}"] <= ${value}`;
            case '$in':
                return `contains(value: r["${field}"], set: ${JSON.stringify(value)})`;
            default:
                throw new Error(`Unknown operator: ${operator}`);
        }
    }
}

module.exports = InfluxDBAdapter;