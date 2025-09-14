
const neo4j = require('neo4j-driver');
const { BaseAdapter } = require('./BaseAdapter');
const { DatabaseError } = require('../utils/errors');

class Neo4jAdapter extends BaseAdapter {
    constructor(config) {
        super();
        this.config = config;
        this.driver = null;
        this.session = null;
        this.models = new Map();
    }

    /**
     * Initialize Neo4j connection
     */
    async initialize() {
        try {
            this.driver = neo4j.driver(
                this.config.uri,
                neo4j.auth.basic(this.config.username, this.config.password),
                {
                    maxConnectionPoolSize: this.config.maxPoolSize || 100,
                    connectionTimeout: this.config.connectionTimeout || 30000
                }
            );

            await this.driver.verifyConnectivity();
            return this.driver;
        } catch (error) {
            throw new DatabaseError(`Neo4j initialization failed: ${error.message}`);
        }
    }

    /**
     * Define a node model
     * @param {string} label - Node label
     * @param {Object} schema - Node properties schema
     */
    defineModel(label, schema) {
        try {
            const model = {
                label,
                schema: this._validateSchema(schema),
                constraints: this._extractConstraints(schema)
            };

            this.models.set(label, model);
            return this._createConstraints(label, model.constraints);
        } catch (error) {
            throw new DatabaseError(`Failed to define model: ${error.message}`);
        }
    }

    /**
     * Create a node
     * @param {string} label - Node label
     * @param {Object} properties - Node properties
     */
    async createNode(label, properties) {
        const session = this.driver.session();
        try {
            const model = this._getModel(label);
            const validatedProps = this._validateProperties(properties, model.schema);

            const result = await session.executeWrite(tx => 
                tx.run(
                    `CREATE (n:${label} $props) RETURN n`,
                    { props: validatedProps }
                )
            );

            return this._processNodeResult(result.records[0].get('n'));
        } catch (error) {
            throw new DatabaseError(`Create node failed: ${error.message}`);
        } finally {
            await session.close();
        }
    }

    /**
     * Create a relationship between nodes
     * @param {string} fromNodeLabel - Source node label
     * @param {string} fromNodeId - Source node ID
     * @param {string} toNodeLabel - Target node label
     * @param {string} toNodeId - Target node ID
     * @param {string} relationType - Relationship type
     * @param {Object} properties - Relationship properties
     */
    async createRelationship(fromNodeLabel, fromNodeId, toNodeLabel, toNodeId, relationType, properties = {}) {
        const session = this.driver.session();
        try {
            const result = await session.executeWrite(tx =>
                tx.run(
                    `
                    MATCH (from:${fromNodeLabel} {id: $fromId})
                    MATCH (to:${toNodeLabel} {id: $toId})
                    CREATE (from)-[r:${relationType} $props]->(to)
                    RETURN r
                    `,
                    {
                        fromId: fromNodeId,
                        toId: toNodeId,
                        props: properties
                    }
                )
            );

            return this._processRelationshipResult(result.records[0].get('r'));
        } catch (error) {
            throw new DatabaseError(`Create relationship failed: ${error.message}`);
        } finally {
            await session.close();
        }
    }

    /**
     * Find nodes
     * @param {string} label - Node label
     * @param {Object} query - Query parameters
     */
    async findNodes(label, query = {}) {
        const session = this.driver.session();
        try {
            const { where, params } = this._buildWhereClause(query);
            
            const result = await session.executeRead(tx =>
                tx.run(
                    `
                    MATCH (n:${label})
                    ${where ? `WHERE ${where}` : ''}
                    RETURN n
                    ${query.orderBy ? `ORDER BY n.${query.orderBy} ${query.order || 'ASC'}` : ''}
                    ${query.limit ? `LIMIT ${query.limit}` : ''}
                    `,
                    params
                )
            );

            return result.records.map(record => 
                this._processNodeResult(record.get('n'))
            );
        } catch (error) {
            throw new DatabaseError(`Find nodes failed: ${error.message}`);
        } finally {
            await session.close();
        }
    }

    /**
     * Find relationships
     * @param {string} relationType - Relationship type
     * @param {Object} query - Query parameters
     */
    async findRelationships(relationType, query = {}) {
        const session = this.driver.session();
        try {
            const { where, params } = this._buildWhereClause(query, 'r');

            const result = await session.executeRead(tx =>
                tx.run(
                    `
                    MATCH ()-[r:${relationType}]->()
                    ${where ? `WHERE ${where}` : ''}
                    RETURN r
                    `,
                    params
                )
            );

            return result.records.map(record =>
                this._processRelationshipResult(record.get('r'))
            );
        } catch (error) {
            throw new DatabaseError(`Find relationships failed: ${error.message}`);
        } finally {
            await session.close();
        }
    }

    /**
     * Execute a Cypher query
     * @param {string} query - Cypher query
     * @param {Object} params - Query parameters
     */
    async query(query, params = {}) {
        const session = this.driver.session();
        try {
            const result = await session.executeRead(tx =>
                tx.run(query, params)
            );

            return result.records.map(record => {
                const obj = {};
                for (const key of record.keys) {
                    const value = record.get(key);
                    obj[key] = this._processValue(value);
                }
                return obj;
            });
        } catch (error) {
            throw new DatabaseError(`Query execution failed: ${error.message}`);
        } finally {
            await session.close();
        }
    }

    /**
     * Update node properties
     * @param {string} label - Node label
     * @param {string} id - Node ID
     * @param {Object} properties - Properties to update
     */
    async updateNode(label, id, properties) {
        const session = this.driver.session();
        try {
            const model = this._getModel(label);
            const validatedProps = this._validateProperties(properties, model.schema);

            const result = await session.executeWrite(tx =>
                tx.run(
                    `
                    MATCH (n:${label} {id: $id})
                    SET n += $props
                    RETURN n
                    `,
                    { id, props: validatedProps }
                )
            );

            return this._processNodeResult(result.records[0].get('n'));
        } catch (error) {
            throw new DatabaseError(`Update node failed: ${error.message}`);
        } finally {
            await session.close();
        }
    }

    /**
     * Delete node
     * @param {string} label - Node label
     * @param {string} id - Node ID
     */
    async deleteNode(label, id) {
        const session = this.driver.session();
        try {
            await session.executeWrite(tx =>
                tx.run(
                    `
                    MATCH (n:${label} {id: $id})
                    DETACH DELETE n
                    `,
                    { id }
                )
            );
            return true;
        } catch (error) {
            throw new DatabaseError(`Delete node failed: ${error.message}`);
        } finally {
            await session.close();
        }
    }

    /**
     * Execute a transaction
     * @param {Function} callback - Transaction callback
     */
    async transaction(callback) {
        const session = this.driver.session();
        try {
            return await session.executeTransaction(tx => callback(tx));
        } catch (error) {
            throw new DatabaseError(`Transaction failed: ${error.message}`);
        } finally {
            await session.close();
        }
    }

    /**
     * Close connection
     */
    async close() {
        try {
            await this.driver.close();
        } catch (error) {
            throw new DatabaseError(`Close failed: ${error.message}`);
        }
    }

    // Private helper methods
    _validateSchema(schema) {
        const validTypes = ['string', 'number', 'boolean', 'date'];
        
        for (const [field, definition] of Object.entries(schema)) {
            if (!validTypes.includes(definition.type)) {
                throw new Error(`Invalid type for field ${field}`);
            }
        }
        
        return schema;
    }

    _extractConstraints(schema) {
        const constraints = [];
        
        for (const [field, definition] of Object.entries(schema)) {
            if (definition.unique) {
                constraints.push({
                    type: 'UNIQUE',
                    field
                });
            }
        }
        
        return constraints;
    }

    async _createConstraints(label, constraints) {
        const session = this.driver.session();
        try {
            for (const constraint of constraints) {
                await session.executeWrite(tx =>
                    tx.run(
                        `CREATE CONSTRAINT IF NOT EXISTS FOR (n:${label}) REQUIRE n.${constraint.field} IS UNIQUE`
                    )
                );
            }
        } finally {
            await session.close();
        }
    }

    _getModel(label) {
        const model = this.models.get(label);
        if (!model) {
            throw new Error(`Model ${label} not found`);
        }
        return model;
    }

    _validateProperties(properties, schema) {
        const validated = {};
        
        for (const [key, value] of Object.entries(properties)) {
            const fieldSchema = schema[key];
            if (fieldSchema) {
                validated[key] = this._convertValue(value, fieldSchema.type);
            }
        }
        
        return validated;
    }

    _convertValue(value, type) {
        if (value === null || value === undefined) return null;
        
        switch (type) {
            case 'string':
                return String(value);
            case 'number':
                return Number(value);
            case 'boolean':
                return Boolean(value);
            case 'date':
                return new Date(value);
            default:
                return value;
        }
    }

    _buildWhereClause(query, prefix = 'n') {
        const conditions = [];
        const params = {};
        
        for (const [key, value] of Object.entries(query)) {
            if (key !== 'orderBy' && key !== 'order' && key !== 'limit') {
                conditions.push(`${prefix}.${key} = $${key}`);
                params[key] = value;
            }
        }
        
        return {
            where: conditions.length > 0 ? conditions.join(' AND ') : '',
            params
        };
    }

    _processNodeResult(node) {
        if (!node) return null;
        return {
            id: node.properties.id,
            labels: node.labels,
            ...node.properties
        };
    }

    _processRelationshipResult(rel) {
        if (!rel) return null;
        return {
            id: rel.identity.toString(),
            type: rel.type,
            properties: rel.properties,
            startNodeId: rel.start.toString(),
            endNodeId: rel.end.toString()
        };
    }

    _processValue(value) {
        if (neo4j.isInt(value)) {
            return value.toNumber();
        }
        if (neo4j.isNode(value)) {
            return this._processNodeResult(value);
        }
        if (neo4j.isRelationship(value)) {
            return this._processRelationshipResult(value);
        }
        return value;
    }
}

module.exports = Neo4jAdapter;