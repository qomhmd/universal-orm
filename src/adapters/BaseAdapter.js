/**
 * Base adapter class providing interface for database operations.
 * All database adapters should extend this class and implement its methods.
 */
class BaseAdapter {
    /**
     * Create a new record in the database
     * @param {string} model - Model/collection/table name
     * @param {Object|Array} data - Data to create
     * @param {Object} [options] - Additional options for creation
     * @returns {Promise<Object|Array>} Created record(s)
     * @throws {Error} If not implemented by child class
     */
    async create(model, data, options = {}) {
        throw new Error('Not implemented');
    }

    /**
     * Find a single record
     * @param {string} model - Model/collection/table name
     * @param {Object} query - Query conditions
     * @param {Object} [options] - Additional query options
     * @returns {Promise<Object|null>} Found record or null
     * @throws {Error} If not implemented by child class
     */
    async findOne(model, query, options = {}) {
        throw new Error('Not implemented');
    }

    /**
     * Find multiple records
     * @param {string} model - Model/collection/table name
     * @param {Object} query - Query conditions
     * @param {Object} [options] - Additional query options
     * @returns {Promise<Array>} Array of found records
     * @throws {Error} If not implemented by child class
     */
    async findMany(model, query = {}, options = {}) {
        throw new Error('Not implemented');
    }

    /**
     * Update records
     * @param {string} model - Model/collection/table name
     * @param {Object} query - Query conditions
     * @param {Object} data - Update data
     * @param {Object} [options] - Additional update options
     * @returns {Promise<Object>} Update result
     * @throws {Error} If not implemented by child class
     */
    async update(model, query, data, options = {}) {
        throw new Error('Not implemented');
    }

    /**
     * Delete records
     * @param {string} model - Model/collection/table name
     * @param {Object} query - Query conditions
     * @param {Object} [options] - Additional delete options
     * @returns {Promise<Object>} Delete result
     * @throws {Error} If not implemented by child class
     */
    async delete(model, query, options = {}) {
        throw new Error('Not implemented');
    }

    /**
     * Execute operations in a transaction
     * @param {Function} callback - Transaction callback
     * @param {Object} [options] - Transaction options
     * @returns {Promise<any>} Transaction result
     * @throws {Error} If not implemented by child class
     */
    async transaction(callback, options = {}) {
        throw new Error('Not implemented');
    }

    /**
     * Close database connection
     * @returns {Promise<void>}
     * @throws {Error} If not implemented by child class
     */
    async close() {
        throw new Error('Not implemented');
    }

    /**
     * Initialize connection to database
     * @param {Object} [options] - Connection options
     * @returns {Promise<void>}
     * @throws {Error} If not implemented by child class
     */
    async initialize(options = {}) {
        throw new Error('Not implemented');
    }

    /**
     * Create a new model/collection/table
     * @param {string} model - Model/collection/table name
     * @param {Object} schema - Schema definition
     * @param {Object} [options] - Additional creation options
     * @returns {Promise<void>}
     * @throws {Error} If not implemented by child class
     */
    async createModel(model, schema, options = {}) {
        throw new Error('Not implemented');
    }

    /**
     * Drop a model/collection/table
     * @param {string} model - Model/collection/table name
     * @param {Object} [options] - Additional drop options
     * @returns {Promise<void>}
     * @throws {Error} If not implemented by child class
     */
    async dropModel(model, options = {}) {
        throw new Error('Not implemented');
    }

    /**
     * Count records
     * @param {string} model - Model/collection/table name
     * @param {Object} query - Query conditions
     * @param {Object} [options] - Additional count options
     * @returns {Promise<number>} Count of matching records
     * @throws {Error} If not implemented by child class
     */
    async count(model, query = {}, options = {}) {
        throw new Error('Not implemented');
    }

    /**
     * Check if records exist
     * @param {string} model - Model/collection/table name
     * @param {Object} query - Query conditions
     * @param {Object} [options] - Additional options
     * @returns {Promise<boolean>} Whether matching records exist
     * @throws {Error} If not implemented by child class
     */
    async exists(model, query, options = {}) {
        throw new Error('Not implemented');
    }

    /**
     * Bulk insert records
     * @param {string} model - Model/collection/table name
     * @param {Array} data - Array of records to insert
     * @param {Object} [options] - Additional bulk insert options
     * @returns {Promise<Object>} Bulk insert result
     * @throws {Error} If not implemented by child class
     */
    async bulkCreate(model, data, options = {}) {
        throw new Error('Not implemented');
    }

    /**
     * Bulk update records
     * @param {string} model - Model/collection/table name
     * @param {Array} updates - Array of update operations
     * @param {Object} [options] - Additional bulk update options
     * @returns {Promise<Object>} Bulk update result
     * @throws {Error} If not implemented by child class
     */
    async bulkUpdate(model, updates, options = {}) {
        throw new Error('Not implemented');
    }

    /**
     * Perform aggregation
     * @param {string} model - Model/collection/table name
     * @param {Array|Object} pipeline - Aggregation pipeline/query
     * @param {Object} [options] - Additional aggregation options
     * @returns {Promise<Array>} Aggregation results
     * @throws {Error} If not implemented by child class
     */
    async aggregate(model, pipeline, options = {}) {
        throw new Error('Not implemented');
    }

    /**
     * Execute raw query
     * @param {string|Object} query - Raw query string or query object
     * @param {Array|Object} [params] - Query parameters
     * @param {Object} [options] - Additional query options
     * @returns {Promise<any>} Query results
     * @throws {Error} If not implemented by child class
     */
    async query(query, params = {}, options = {}) {
        throw new Error('Not implemented');
    }
}

module.exports = BaseAdapter;