import mongoose, { Model, Schema, Connection, Document } from 'mongoose';
import { BaseAdapter, DatabaseConfig, QueryOptions, CreateOptions, UpdateOptions, DeleteOptions, TransactionOptions, ModelSchema, BulkUpdateOperation } from './BaseAdapter';
import { DatabaseError } from '../utils/errors';

interface MongoQueryOptions extends QueryOptions {
  select?: string | string[];
  populate?: string | string[];
}

interface MongoUpdateOptions extends UpdateOptions {
  new?: boolean;
}

export default class MongoAdapter extends BaseAdapter {
  private models: Map<string, Model<any>>;
  private connection: Connection | null;

  constructor(config: DatabaseConfig) {
    super(config);
    this.models = new Map();
    this.connection = null;
  }

  /**
   * Initialize MongoDB connection
   */
  async initialize(options?: Record<string, any>): Promise<void> {
    try {
      this.connection = await mongoose.createConnection(this.config.uri as string);
      // Connection error handling
      this.connection.on('error', (error) => {
        throw new DatabaseError(`MongoDB connection error: ${error.message}`);
      });
    } catch (error) {
      throw new DatabaseError(`MongoDB initialization failed: ${(error as Error).message}`);
    }
  }

  /**
   * Create a new record in the database
   */
  async create(model: string, data: Record<string, any> | Record<string, any>[], options?: CreateOptions): Promise<Record<string, any> | Record<string, any>[]> {
    try {
      const modelInstance = this._getModel(model);
      const result = await modelInstance.create(data, options);
      if (Array.isArray(result)) {
        return result.map((doc: any) => doc.toObject());
      }
      return (result as any).toObject();
    } catch (error) {
      throw new DatabaseError(`Create operation failed: ${(error as Error).message}`);
    }
  }

  /**
   * Find a single record
   */
  async findOne(model: string, query: Record<string, any>, options?: MongoQueryOptions): Promise<Record<string, any> | null> {
    try {
      const modelInstance = this._getModel(model);
      const { select, populate } = options || {};

      let mongoQuery = modelInstance.findOne(this._convertQuery(query));

      if (select) mongoQuery = mongoQuery.select(select);
      if (populate) mongoQuery = mongoQuery.populate(populate);

      const result = await mongoQuery.exec();
      return result ? result.toObject() : null;
    } catch (error) {
      throw new DatabaseError(`Find operation failed: ${(error as Error).message}`);
    }
  }

  /**
   * Find multiple records
   */
  async findMany(model: string, query: Record<string, any> = {}, options?: MongoQueryOptions): Promise<Record<string, any>[]> {
    try {
      const modelInstance = this._getModel(model);
      const {
        select,
        populate,
        limit,
        offset,
        sort
      } = options || {};

      let mongoQuery = modelInstance.find(this._convertQuery(query));

      if (select) mongoQuery = mongoQuery.select(select);
      if (populate) mongoQuery = mongoQuery.populate(populate);
      if (limit) mongoQuery = mongoQuery.limit(limit);
      if (offset) mongoQuery = mongoQuery.skip(offset);
      if (sort) mongoQuery = mongoQuery.sort(sort);

      const results = await mongoQuery.exec();
      return results.map((doc: any) => doc.toObject());
    } catch (error) {
      throw new DatabaseError(`Find operation failed: ${(error as Error).message}`);
    }
  }

  /**
   * Update records
   */
  async update(model: string, query: Record<string, any>, data: Record<string, any>, options?: MongoUpdateOptions): Promise<Record<string, any>> {
    try {
      const modelInstance = this._getModel(model);
      const result = await modelInstance.updateMany(
        this._convertQuery(query),
        this._convertUpdateOperations(data),
        options
      );
      return result;
    } catch (error) {
      throw new DatabaseError(`Update operation failed: ${(error as Error).message}`);
    }
  }

  /**
   * Delete records
   */
  async delete(model: string, query: Record<string, any>, options?: DeleteOptions): Promise<Record<string, any>> {
    try {
      const modelInstance = this._getModel(model);
      return await modelInstance.deleteMany(this._convertQuery(query));
    } catch (error) {
      throw new DatabaseError(`Delete operation failed: ${(error as Error).message}`);
    }
  }

  /**
   * Execute operations in a transaction
   */
  async transaction<T>(callback: () => Promise<T>, options?: TransactionOptions): Promise<T> {
    if (!this.connection) {
      throw new DatabaseError('Database connection not initialized');
    }

    const session = await this.connection.startSession();
    try {
      session.startTransaction();
      const result = await callback();
      await session.commitTransaction();
      return result;
    } catch (error) {
      await session.abortTransaction();
      throw new DatabaseError(`Transaction failed: ${(error as Error).message}`);
    } finally {
      session.endSession();
    }
  }

  /**
   * Close database connection
   */
  async close(): Promise<void> {
    if (this.connection) {
      await this.connection.close();
      this.connection = null;
    }
  }

  /**
   * Create a new model/collection/table
   */
  async createModel(model: string, schema: ModelSchema, options?: Record<string, any>): Promise<void> {
    try {
      if (!this.connection) {
        throw new Error('Database connection not initialized');
      }

      const mongooseSchema = new Schema(
        this._convertSchema(schema),
        {
          timestamps: true,
          strict: true,
          ...options
        }
      );

      // Add indexes
      this._addSchemaIndexes(mongooseSchema, schema);

      const modelInstance = this.connection.model(model, mongooseSchema);
      this.models.set(model, modelInstance);
    } catch (error) {
      throw new DatabaseError(`Failed to create model: ${(error as Error).message}`);
    }
  }

  /**
   * Drop a model/collection/table
   */
  async dropModel(model: string, options?: Record<string, any>): Promise<void> {
    try {
      if (!this.connection) {
        throw new Error('Database connection not initialized');
      }

      const conn = this.connection;
      if (!conn.db) {
        throw new Error('Database not available');
      }
      await conn.db.dropCollection(model);
      this.models.delete(model);
    } catch (error) {
      throw new DatabaseError(`Failed to drop model: ${(error as Error).message}`);
    }
  }

  /**
   * Count records
   */
  async count(model: string, query: Record<string, any> = {}, options?: QueryOptions): Promise<number> {
    try {
      const modelInstance = this._getModel(model);
      return await modelInstance.countDocuments(this._convertQuery(query));
    } catch (error) {
      throw new DatabaseError(`Count operation failed: ${(error as Error).message}`);
    }
  }

  /**
   * Check if records exist
   */
  async exists(model: string, query: Record<string, any>, options?: QueryOptions): Promise<boolean> {
    try {
      const count = await this.count(model, query, options);
      return count > 0;
    } catch (error) {
      throw new DatabaseError(`Exists operation failed: ${(error as Error).message}`);
    }
  }

  /**
   * Bulk insert records
   */
  async bulkCreate(model: string, data: Record<string, any>[], options?: CreateOptions): Promise<Record<string, any>> {
    try {
      const modelInstance = this._getModel(model);
      const result = await modelInstance.insertMany(data, options || {});
      return { insertedCount: result.length, insertedIds: result.map((doc: any) => doc._id) };
    } catch (error) {
      throw new DatabaseError(`Bulk create operation failed: ${(error as Error).message}`);
    }
  }

  /**
   * Bulk update records
   */
  async bulkUpdate(model: string, updates: BulkUpdateOperation[], options?: UpdateOptions): Promise<Record<string, any>> {
    try {
      const modelInstance = this._getModel(model);
      const bulkOps = updates.map(update => ({
        updateMany: {
          filter: this._convertQuery(update.query),
          update: this._convertUpdateOperations(update.update),
          ...update.options
        }
      }));

      const result = await modelInstance.bulkWrite(bulkOps, options as any);
      return result;
    } catch (error) {
      throw new DatabaseError(`Bulk update operation failed: ${(error as Error).message}`);
    }
  }

  /**
   * Perform aggregation
   */
  async aggregate(model: string, pipeline: Record<string, any>[] | Record<string, any>, options?: QueryOptions): Promise<Record<string, any>[]> {
    try {
      const modelInstance = this._getModel(model);
      const result = await modelInstance.aggregate(Array.isArray(pipeline) ? pipeline : [pipeline]);
      return result;
    } catch (error) {
      throw new DatabaseError(`Aggregation operation failed: ${(error as Error).message}`);
    }
  }

  /**
   * Execute raw query
   */
  async query(query: string | Record<string, any>, params?: Record<string, any> | any[], options?: QueryOptions): Promise<any> {
    try {
      if (!this.connection) {
        throw new Error('Database connection not initialized');
      }

      const conn = this.connection;
      if (typeof query === 'string') {
        // Raw MongoDB command
        if (!conn.db) {
          throw new Error('Database not available');
        }
        return await conn.db.command(JSON.parse(query));
      } else {
        // Direct collection operation
        if (!conn.db) {
          throw new Error('Database not available');
        }
        const collection = conn.db.collection(query.collection as string);
        const operation = query.operation as string;
        const method = (collection as any)[operation];
        if (typeof method === 'function') {
          const args = Array.isArray(params) ? params : [params];
          return await method.apply(collection, args);
        }
        throw new Error(`Unknown operation: ${operation}`);
      }
    } catch (error) {
      throw new DatabaseError(`Query operation failed: ${(error as Error).message}`);
    }
  }

  // Private helper methods
  private _getModel(modelName: string): Model<any> {
    const model = this.models.get(modelName);
    if (!model) {
      throw new Error(`Model ${modelName} not found`);
    }
    return model;
  }

  private _convertSchema(schema: ModelSchema): Record<string, any> {
    const mongooseSchema: Record<string, any> = {};

    for (const [field, definition] of Object.entries(schema)) {
      mongooseSchema[field] = this._convertFieldType(definition);
    }

    return mongooseSchema;
  }

  private _convertFieldType(definition: any): Record<string, any> {
    const typeMap: Record<string, any> = {
      'string': String,
      'number': Number,
      'integer': Number,
      'boolean': Boolean,
      'date': Date,
      'json': Schema.Types.Mixed,
      'objectId': Schema.Types.ObjectId
    };

    const schemaType: Record<string, any> = {
      type: typeMap[definition.type] || String,
      required: definition.required === true,
      unique: definition.unique === true,
      default: definition.default,
    };

    if (definition.ref) {
      schemaType.ref = definition.ref;
    }

    return schemaType;
  }

  private _convertQuery(query: Record<string, any>): Record<string, any> {
    const operatorMap: Record<string, string> = {
      $eq: '$eq',
      $ne: '$ne',
      $gt: '$gt',
      $gte: '$gte',
      $lt: '$lt',
      $lte: '$lte',
      $in: '$in',
      $nin: '$nin',
      $regex: '$regex',
      $exists: '$exists'
    };

    return this._processQueryOperators(query, operatorMap);
  }

  private _processQueryOperators(query: Record<string, any>, operatorMap: Record<string, string>): Record<string, any> {
    const processed: Record<string, any> = {};

    for (const [key, value] of Object.entries(query)) {
      if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
        const conditions: Record<string, any> = {};
        for (const [operator, operand] of Object.entries(value)) {
          if (operatorMap[operator]) {
            conditions[operatorMap[operator]] = operand;
          }
        }
        processed[key] = Object.keys(conditions).length > 0 ? conditions : value;
      } else {
        processed[key] = value;
      }
    }

    return processed;
  }

  private _convertUpdateOperations(update: Record<string, any>): Record<string, any> {
    if (update.$set || update.$unset || update.$push || update.$pull) {
      return update;
    }
    return { $set: update };
  }

  private _addSchemaIndexes(mongooseSchema: Schema, schema: ModelSchema): void {
    const indexes: Record<string, 1 | -1>[] = [];

    for (const [field, definition] of Object.entries(schema)) {
      if ((definition as any).index) {
        indexes.push({ [field]: 1 });
      }
    }

    if (indexes.length > 0) {
      mongooseSchema.index(indexes as any);
    }
  }
}