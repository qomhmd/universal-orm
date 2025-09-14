// src/adapters/DynamoDBAdapter.js
const AWS = require('aws-sdk');
const { BaseAdapter } = require('./BaseAdapter');
const { DatabaseError } = require('../utils/errors');

class DynamoDBAdapter extends BaseAdapter {
    constructor(config) {
        super();
        this.config = config;
        this.client = null;
        this.documentClient = null;
        this.models = new Map();
    }

    /**
     * Initialize DynamoDB connection
     */
    async initialize() {
        try {
            AWS.config.update({
                region: this.config.region,
                accessKeyId: this.config.accessKeyId,
                secretAccessKey: this.config.secretAccessKey,
                endpoint: this.config.endpoint // For local development
            });

            this.client = new AWS.DynamoDB();
            this.documentClient = new AWS.DynamoDB.DocumentClient({
                service: this.client,
                convertEmptyValues: true
            });

            // Verify connection
            await this.client.listTables().promise();
            return this.client;
        } catch (error) {
            throw new DatabaseError(`DynamoDB initialization failed: ${error.message}`);
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
                sortKey = null,
                secondaryIndexes = [],
                readCapacity = 5,
                writeCapacity = 5
            } = options;

            const tableParams = {
                TableName: tableName,
                AttributeDefinitions: this._buildAttributeDefinitions(schema, [partitionKey, sortKey, ...secondaryIndexes]),
                KeySchema: this._buildKeySchema(partitionKey, sortKey),
                BillingMode: 'PROVISIONED',
                ProvisionedThroughput: {
                    ReadCapacityUnits: readCapacity,
                    WriteCapacityUnits: writeCapacity
                }
            };

            // Add GSIs if defined
            if (secondaryIndexes.length > 0) {
                tableParams.GlobalSecondaryIndexes = this._buildGlobalSecondaryIndexes(
                    secondaryIndexes,
                    readCapacity,
                    writeCapacity
                );
            }

            // Check if table exists
            const tables = await this.client.listTables().promise();
            if (!tables.TableNames.includes(tableName)) {
                await this.client.createTable(tableParams).promise();
                await this._waitForTableActive(tableName);
            }

            this.models.set(tableName, {
                schema,
                partitionKey,
                sortKey,
                secondaryIndexes
            });

            return true;
        } catch (error) {
            throw new DatabaseError(`Define model failed: ${error.message}`);
        }
    }

    /**
     * Put item
     * @param {string} tableName - Table name
     * @param {Object} item - Item to put
     * @param {Object} options - Put options
     */
    async put(tableName, item, options = {}) {
        try {
            const {
                condition,
                returnValues = 'NONE'
            } = options;

            const model = this._getModel(tableName);
            const validatedItem = this._validateItem(item, model.schema);

            const params = {
                TableName: tableName,
                Item: validatedItem,
                ReturnValues: returnValues
            };

            if (condition) {
                params.ConditionExpression = condition.expression;
                params.ExpressionAttributeNames = condition.names;
                params.ExpressionAttributeValues = condition.values;
            }

            return await this.documentClient.put(params).promise();
        } catch (error) {
            throw new DatabaseError(`Put item failed: ${error.message}`);
        }
    }

    /**
     * Get item
     * @param {string} tableName - Table name
     * @param {Object} key - Primary key
     * @param {Object} options - Get options
     */
    async get(tableName, key, options = {}) {
        try {
            const {
                consistentRead = false,
                projectionExpression
            } = options;

            const params = {
                TableName: tableName,
                Key: key,
                ConsistentRead: consistentRead
            };

            if (projectionExpression) {
                params.ProjectionExpression = projectionExpression;
            }

            const result = await this.documentClient.get(params).promise();
            return result.Item || null;
        } catch (error) {
            throw new DatabaseError(`Get item failed: ${error.message}`);
        }
    }

    /**
     * Query items
     * @param {string} tableName - Table name
     * @param {Object} query - Query parameters
     * @param {Object} options - Query options
     */
    async query(tableName, query, options = {}) {
        try {
            const {
                index,
                select,
                limit,
                scanIndexForward = true,
                consistentRead = false,
                lastEvaluatedKey
            } = options;

            const { expression, names, values } = this._buildQueryExpression(query);

            const params = {
                TableName: tableName,
                KeyConditionExpression: expression,
                ExpressionAttributeNames: names,
                ExpressionAttributeValues: values,
                ScanIndexForward: scanIndexForward,
                ConsistentRead: consistentRead
            };

            if (index) params.IndexName = index;
            if (select) params.ProjectionExpression = select;
            if (limit) params.Limit = limit;
            if (lastEvaluatedKey) params.ExclusiveStartKey = lastEvaluatedKey;

            const result = await this.documentClient.query(params).promise();
            return {
                items: result.Items,
                count: result.Count,
                scannedCount: result.ScannedCount,
                lastEvaluatedKey: result.LastEvaluatedKey
            };
        } catch (error) {
            throw new DatabaseError(`Query failed: ${error.message}`);
        }
    }

    /**
     * Scan items
     * @param {string} tableName - Table name
     * @param {Object} filter - Filter conditions
     * @param {Object} options - Scan options
     */
    async scan(tableName, filter = {}, options = {}) {
        try {
            const {
                index,
                select,
                limit,
                segment,
                totalSegments,
                lastEvaluatedKey
            } = options;

            const { expression, names, values } = this._buildFilterExpression(filter);

            const params = {
                TableName: tableName,
                FilterExpression: expression,
                ExpressionAttributeNames: names,
                ExpressionAttributeValues: values
            };

            if (index) params.IndexName = index;
            if (select) params.ProjectionExpression = select;
            if (limit) params.Limit = limit;
            if (segment !== undefined && totalSegments) {
                params.Segment = segment;
                params.TotalSegments = totalSegments;
            }
            if (lastEvaluatedKey) params.ExclusiveStartKey = lastEvaluatedKey;

            const result = await this.documentClient.scan(params).promise();
            return {
                items: result.Items,
                count: result.Count,
                scannedCount: result.ScannedCount,
                lastEvaluatedKey: result.LastEvaluatedKey
            };
        } catch (error) {
            throw new DatabaseError(`Scan failed: ${error.message}`);
        }
    }

    /**
     * Update item
     * @param {string} tableName - Table name
     * @param {Object} key - Primary key
     * @param {Object} update - Update operations
     * @param {Object} options - Update options
     */
    async update(tableName, key, update, options = {}) {
        try {
            const {
                condition,
                returnValues = 'ALL_NEW'
            } = options;

            const { expression, names, values } = this._buildUpdateExpression(update);

            const params = {
                TableName: tableName,
                Key: key,
                UpdateExpression: expression,
                ExpressionAttributeNames: names,
                ExpressionAttributeValues: values,
                ReturnValues: returnValues
            };

            if (condition) {
                params.ConditionExpression = condition.expression;
                Object.assign(params.ExpressionAttributeNames, condition.names);
                Object.assign(params.ExpressionAttributeValues, condition.values);
            }

            const result = await this.documentClient.update(params).promise();
            return result.Attributes;
        } catch (error) {
            throw new DatabaseError(`Update item failed: ${error.message}`);
        }
    }

    /**
     * Delete item
     * @param {string} tableName - Table name
     * @param {Object} key - Primary key
     * @param {Object} options - Delete options
     */
    async delete(tableName, key, options = {}) {
        try {
            const {
                condition,
                returnValues = 'NONE'
            } = options;

            const params = {
                TableName: tableName,
                Key: key,
                ReturnValues: returnValues
            };

            if (condition) {
                params.ConditionExpression = condition.expression;
                params.ExpressionAttributeNames = condition.names;
                params.ExpressionAttributeValues = condition.values;
            }

            const result = await this.documentClient.delete(params).promise();
            return result.Attributes;
        } catch (error) {
            throw new DatabaseError(`Delete item failed: ${error.message}`);
        }
    }

    /**
     * Batch write items
     * @param {Array} operations - Array of write operations
     */
    async batchWrite(operations) {
        try {
            const batchParams = {
                RequestItems: {}
            };

            // Group operations by table
            for (const op of operations) {
                if (!batchParams.RequestItems[op.table]) {
                    batchParams.RequestItems[op.table] = [];
                }

                if (op.type === 'put') {
                    batchParams.RequestItems[op.table].push({
                        PutRequest: {
                            Item: op.item
                        }
                    });
                } else if (op.type === 'delete') {
                    batchParams.RequestItems[op.table].push({
                        DeleteRequest: {
                            Key: op.key
                        }
                    });
                }
            }

            // Handle batch size limits and unprocessed items
            const results = [];
            let unprocessed = batchParams;

            while (Object.keys(unprocessed.RequestItems).length > 0) {
                const result = await this.documentClient.batchWrite(unprocessed).promise();
                results.push(result);

                if (!result.UnprocessedItems || Object.keys(result.UnprocessedItems).length === 0) {
                    break;
                }

                unprocessed = { RequestItems: result.UnprocessedItems };
                // Add exponential backoff here if needed
            }

            return results;
        } catch (error) {
            throw new DatabaseError(`Batch write failed: ${error.message}`);
        }
    }

    /**
     * Batch get items
     * @param {Array} operations - Array of get operations
     */
    async batchGet(operations) {
        try {
            const batchParams = {
                RequestItems: {}
            };

            // Group operations by table
            for (const op of operations) {
                if (!batchParams.RequestItems[op.table]) {
                    batchParams.RequestItems[op.table] = {
                        Keys: []
                    };
                }

                batchParams.RequestItems[op.table].Keys.push(op.key);

                if (op.projectionExpression) {
                    batchParams.RequestItems[op.table].ProjectionExpression = op.projectionExpression;
                }
            }

            // Handle batch size limits and unprocessed items
            const results = new Map();
            let unprocessed = batchParams;

            while (Object.keys(unprocessed.RequestItems).length > 0) {
                const result = await this.documentClient.batchGet(unprocessed).promise();
                
                // Merge results
                for (const [table, items] of Object.entries(result.Responses)) {
                    if (!results.has(table)) {
                        results.set(table, []);
                    }
                    results.get(table).push(...items);
                }

                if (!result.UnprocessedKeys || Object.keys(result.UnprocessedKeys).length === 0) {
                    break;
                }

                unprocessed = { RequestItems: result.UnprocessedKeys };
                // Add exponential backoff here if needed
            }

            return results;
        } catch (error) {
            throw new DatabaseError(`Batch get failed: ${error.message}`);
        }
    }

    /**
     * Perform a transaction
     * @param {Array} operations - Array of transaction operations
     */
    async transaction(operations) {
        try {
            const transactionParams = {
                TransactItems: operations.map(op => {
                    switch (op.type) {
                        case 'put':
                            return {
                                Put: {
                                    TableName: op.table,
                                    Item: op.item,
                                    ...op.options
                                }
                            };
                        case 'update':
                            const { expression, names, values } = this._buildUpdateExpression(op.update);
                            return {
                                Update: {
                                    TableName: op.table,
                                    Key: op.key,
                                    UpdateExpression: expression,
                                    ExpressionAttributeNames: names,
                                    ExpressionAttributeValues: values,
                                    ...op.options
                                }
                            };
                        case 'delete':
                            return {
                                Delete: {
                                    TableName: op.table,
                                    Key: op.key,
                                    ...op.options
                                }
                            };
                        case 'condition':
                            return {
                                ConditionCheck: {
                                    TableName: op.table,
                                    Key: op.key,
                                    ConditionExpression: op.condition.expression,
                                    ExpressionAttributeNames: op.condition.names,
                                    ExpressionAttributeValues: op.condition.values
                                }
                            };
                        default:
                            throw new Error(`Unknown operation type: ${op.type}`);
                    }
                })
            };

            return await this.documentClient.transactWrite(transactionParams).promise();
        } catch (error) {
            throw new DatabaseError(`Transaction failed: ${error.message}`);
        }
    }

// Continuation of DynamoDBAdapter class private methods

    /**
     * Build attribute definitions for table creation
     */
    _buildAttributeDefinitions(schema, keys) {
        const definitions = [];
        const processedKeys = new Set();

        for (const key of keys) {
            if (key && !processedKeys.has(key)) {
                definitions.push({
                    AttributeName: key,
                    AttributeType: this._getDynamoDBType(schema[key].type)
                });
                processedKeys.add(key);
            }
        }

        return definitions;
    }

    /**
     * Build key schema for table creation
     */
    _buildKeySchema(partitionKey, sortKey) {
        const keySchema = [
            {
                AttributeName: partitionKey,
                KeyType: 'HASH'
            }
        ];

        if (sortKey) {
            keySchema.push({
                AttributeName: sortKey,
                KeyType: 'RANGE'
            });
        }

        return keySchema;
    }

    /**
     * Build GSI definitions for table creation
     */
    _buildGlobalSecondaryIndexes(indexes, readCapacity, writeCapacity) {
        return indexes.map(index => {
            const [hashKey, rangeKey] = Array.isArray(index) ? index : [index];
            
            return {
                IndexName: `${hashKey}${rangeKey ? `-${rangeKey}` : ''}-index`,
                KeySchema: [
                    { AttributeName: hashKey, KeyType: 'HASH' },
                    ...(rangeKey ? [{ AttributeName: rangeKey, KeyType: 'RANGE' }] : [])
                ],
                Projection: {
                    ProjectionType: 'ALL'
                },
                ProvisionedThroughput: {
                    ReadCapacityUnits: readCapacity,
                    WriteCapacityUnits: writeCapacity
                }
            };
        });
    }

    /**
     * Build query expression
     */
    _buildQueryExpression(query) {
        const names = {};
        const values = {};
        const conditions = [];

        for (const [key, value] of Object.entries(query)) {
            const attrName = `#${key}`;
            const attrValue = `:${key}`;

            names[attrName] = key;

            if (typeof value === 'object' && value !== null) {
                const operator = Object.keys(value)[0];
                const operand = value[operator];
                values[attrValue] = operand;

                switch (operator) {
                    case '$eq':
                        conditions.push(`${attrName} = ${attrValue}`);
                        break;
                    case '$lt':
                        conditions.push(`${attrName} < ${attrValue}`);
                        break;
                    case '$lte':
                        conditions.push(`${attrName} <= ${attrValue}`);
                        break;
                    case '$gt':
                        conditions.push(`${attrName} > ${attrValue}`);
                        break;
                    case '$gte':
                        conditions.push(`${attrName} >= ${attrValue}`);
                        break;
                    case '$between':
                        values[`${attrValue}_start`] = operand[0];
                        values[`${attrValue}_end`] = operand[1];
                        conditions.push(`${attrName} BETWEEN ${attrValue}_start AND ${attrValue}_end`);
                        break;
                    case '$beginsWith':
                        conditions.push(`begins_with(${attrName}, ${attrValue})`);
                        break;
                }
            } else {
                values[attrValue] = value;
                conditions.push(`${attrName} = ${attrValue}`);
            }
        }

        return {
            expression: conditions.join(' AND '),
            names,
            values
        };
    }

    /**
     * Build filter expression
     */
    _buildFilterExpression(filter) {
        const names = {};
        const values = {};
        const conditions = [];

        for (const [key, value] of Object.entries(filter)) {
            const attrName = `#${key}`;
            const attrValue = `:${key}`;

            names[attrName] = key;

            if (typeof value === 'object' && value !== null) {
                for (const [operator, operand] of Object.entries(value)) {
                    const valueKey = `${attrValue}_${operator}`;
                    values[valueKey] = operand;

                    switch (operator) {
                        case '$eq':
                            conditions.push(`${attrName} = ${valueKey}`);
                            break;
                        case '$ne':
                            conditions.push(`${attrName} <> ${valueKey}`);
                            break;
                        case '$in':
                            conditions.push(`${attrName} IN (${operand.map((_, i) => `${valueKey}_${i}`).join(', ')})`);
                            operand.forEach((val, i) => values[`${valueKey}_${i}`] = val);
                            break;
                        case '$contains':
                            conditions.push(`contains(${attrName}, ${valueKey})`);
                            break;
                        case '$exists':
                            conditions.push(operand ? `attribute_exists(${attrName})` : `attribute_not_exists(${attrName})`);
                            delete values[valueKey];
                            break;
                        case '$type':
                            conditions.push(`attribute_type(${attrName}, ${valueKey})`);
                            break;
                    }
                }
            } else {
                values[attrValue] = value;
                conditions.push(`${attrName} = ${attrValue}`);
            }
        }

        return {
            expression: conditions.length > 0 ? conditions.join(' AND ') : undefined,
            names: Object.keys(names).length > 0 ? names : undefined,
            values: Object.keys(values).length > 0 ? values : undefined
        };
    }

    /**
     * Build update expression
     */
    _buildUpdateExpression(update) {
        const names = {};
        const values = {};
        const sections = {
            SET: [],
            REMOVE: [],
            ADD: [],
            DELETE: []
        };

        for (const [key, value] of Object.entries(update)) {
            const attrName = `#${key}`;
            const attrValue = `:${key}`;
            names[attrName] = key;

            if (value === null || value === undefined) {
                sections.REMOVE.push(attrName);
            } else if (typeof value === 'object' && !Array.isArray(value)) {
                const operator = Object.keys(value)[0];
                const operand = value[operator];

                switch (operator) {
                    case '$set':
                        values[attrValue] = operand;
                        sections.SET.push(`${attrName} = ${attrValue}`);
                        break;
                    case '$remove':
                        sections.REMOVE.push(attrName);
                        break;
                    case '$add':
                        values[attrValue] = operand;
                        sections.ADD.push(`${attrName} ${attrValue}`);
                        break;
                    case '$delete':
                        values[attrValue] = operand;
                        sections.DELETE.push(`${attrName} ${attrValue}`);
                        break;
                    case '$append':
                        values[attrValue] = operand;
                        sections.SET.push(`${attrName} = list_append(${attrName}, ${attrValue})`);
                        break;
                    case '$prepend':
                        values[attrValue] = operand;
                        sections.SET.push(`${attrName} = list_append(${attrValue}, ${attrName})`);
                        break;
                }
            } else {
                values[attrValue] = value;
                sections.SET.push(`${attrName} = ${attrValue}`);
            }
        }

        const expression = Object.entries(sections)
            .filter(([_, items]) => items.length > 0)
            .map(([action, items]) => `${action} ${items.join(', ')}`)
            .join(' ');

        return {
            expression,
            names,
            values
        };
    }

    /**
     * Get DynamoDB attribute type
     */
    _getDynamoDBType(type) {
        const typeMap = {
            'string': 'S',
            'number': 'N',
            'binary': 'B',
            'boolean': 'BOOL',
            'null': 'NULL',
            'list': 'L',
            'map': 'M',
            'string_set': 'SS',
            'number_set': 'NS',
            'binary_set': 'BS'
        };

        return typeMap[type] || 'S';
    }

    /**
     * Validate item against schema
     */
    _validateItem(item, schema) {
        const validated = {};
        
        for (const [key, value] of Object.entries(item)) {
            const fieldSchema = schema[key];
            if (fieldSchema) {
                validated[key] = this._convertValue(value, fieldSchema.type);
            } else {
                validated[key] = value; // Allow additional fields
            }
        }

        return validated;
    }

    /**
     * Convert value to appropriate type
     */
    _convertValue(value, type) {
        if (value === null || value === undefined) {
            return null;
        }

        switch (type) {
            case 'number':
                return Number(value);
            case 'boolean':
                return Boolean(value);
            case 'string':
                return String(value);
            case 'list':
                return Array.isArray(value) ? value : [value];
            case 'map':
                return typeof value === 'object' ? value : {};
            default:
                return value;
        }
    }

    /**
     * Wait for table to become active
     */
    async _waitForTableActive(tableName) {
        let tableActive = false;
        const maxRetries = 10;
        let retries = 0;

        while (!tableActive && retries < maxRetries) {
            const result = await this.client.describeTable({
                TableName: tableName
            }).promise();

            tableActive = result.Table.TableStatus === 'ACTIVE';

            if (!tableActive) {
                await new Promise(resolve => setTimeout(resolve, 1000));
                retries++;
            }
        }

        if (!tableActive) {
            throw new Error('Table did not become active within the expected time');
        }
    }

    /**
     * Get model
     */
    _getModel(tableName) {
        const model = this.models.get(tableName);
        if (!model) {
            throw new Error(`Model ${tableName} not found`);
        }
        return model;
    }
}

module.exports = DynamoDBAdapter;