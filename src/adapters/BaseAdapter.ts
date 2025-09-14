/**
 * Base adapter class providing interface for database operations.
 * All database adapters should extend this class and implement its methods.
 */

export interface DatabaseConfig {
  [key: string]: any;
}

export interface QueryOptions {
  limit?: number;
  offset?: number;
  sort?: Record<string, 1 | -1>;
  fields?: string[];
  [key: string]: any;
}

export interface CreateOptions extends QueryOptions {
  [key: string]: any;
}

export interface UpdateOptions extends QueryOptions {
  upsert?: boolean;
  [key: string]: any;
}

export interface DeleteOptions extends QueryOptions {
  [key: string]: any;
}

export interface TransactionOptions {
  isolation?: string;
  [key: string]: any;
}

export interface ModelSchema {
  [key: string]: any;
}

export interface BulkUpdateOperation {
  query: Record<string, any>;
  update: Record<string, any>;
  options?: UpdateOptions;
}

export abstract class BaseAdapter {
  protected config: DatabaseConfig;

  constructor(config: DatabaseConfig) {
    this.config = config;
  }

  /**
   * Create a new record in the database
   * @param model - Model/collection/table name
   * @param data - Data to create
   * @param options - Additional options for creation
   * @returns Created record(s)
   */
  abstract create(model: string, data: Record<string, any> | Record<string, any>[], options?: CreateOptions): Promise<Record<string, any> | Record<string, any>[]>;

  /**
   * Find a single record
   * @param model - Model/collection/table name
   * @param query - Query conditions
   * @param options - Additional query options
   * @returns Found record or null
   */
  abstract findOne(model: string, query: Record<string, any>, options?: QueryOptions): Promise<Record<string, any> | null>;

  /**
   * Find multiple records
   * @param model - Model/collection/table name
   * @param query - Query conditions
   * @param options - Additional query options
   * @returns Array of found records
   */
  abstract findMany(model: string, query?: Record<string, any>, options?: QueryOptions): Promise<Record<string, any>[]>;

  /**
   * Update records
   * @param model - Model/collection/table name
   * @param query - Query conditions
   * @param data - Update data
   * @param options - Additional update options
   * @returns Update result
   */
  abstract update(model: string, query: Record<string, any>, data: Record<string, any>, options?: UpdateOptions): Promise<Record<string, any>>;

  /**
   * Delete records
   * @param model - Model/collection/table name
   * @param query - Query conditions
   * @param options - Additional delete options
   * @returns Delete result
   */
  abstract delete(model: string, query: Record<string, any>, options?: DeleteOptions): Promise<Record<string, any>>;

  /**
   * Execute operations in a transaction
   * @param callback - Transaction callback
   * @param options - Transaction options
   * @returns Transaction result
   */
  abstract transaction<T>(callback: () => Promise<T>, options?: TransactionOptions): Promise<T>;

  /**
   * Close database connection
   */
  abstract close(): Promise<void>;

  /**
   * Initialize connection to database
   * @param options - Connection options
   */
  abstract initialize(options?: Record<string, any>): Promise<void>;

  /**
   * Create a new model/collection/table
   * @param model - Model/collection/table name
   * @param schema - Schema definition
   * @param options - Additional creation options
   */
  abstract createModel(model: string, schema: ModelSchema, options?: Record<string, any>): Promise<void>;

  /**
   * Drop a model/collection/table
   * @param model - Model/collection/table name
   * @param options - Additional drop options
   */
  abstract dropModel(model: string, options?: Record<string, any>): Promise<void>;

  /**
   * Count records
   * @param model - Model/collection/table name
   * @param query - Query conditions
   * @param options - Additional count options
   * @returns Count of matching records
   */
  abstract count(model: string, query?: Record<string, any>, options?: QueryOptions): Promise<number>;

  /**
   * Check if records exist
   * @param model - Model/collection/table name
   * @param query - Query conditions
   * @param options - Additional options
   * @returns Whether matching records exist
   */
  abstract exists(model: string, query: Record<string, any>, options?: QueryOptions): Promise<boolean>;

  /**
   * Bulk insert records
   * @param model - Model/collection/table name
   * @param data - Array of records to insert
   * @param options - Additional bulk insert options
   * @returns Bulk insert result
   */
  abstract bulkCreate(model: string, data: Record<string, any>[], options?: CreateOptions): Promise<Record<string, any>>;

  /**
   * Bulk update records
   * @param model - Model/collection/table name
   * @param updates - Array of update operations
   * @param options - Additional bulk update options
   * @returns Bulk update result
   */
  abstract bulkUpdate(model: string, updates: BulkUpdateOperation[], options?: UpdateOptions): Promise<Record<string, any>>;

  /**
   * Perform aggregation
   * @param model - Model/collection/table name
   * @param pipeline - Aggregation pipeline/query
   * @param options - Additional aggregation options
   * @returns Aggregation results
   */
  abstract aggregate(model: string, pipeline: Record<string, any>[] | Record<string, any>, options?: QueryOptions): Promise<Record<string, any>[]>;

  /**
   * Execute raw query
   * @param query - Raw query string or query object
   * @param params - Query parameters
   * @param options - Additional query options
   * @returns Query results
   */
  abstract query(query: string | Record<string, any>, params?: Record<string, any> | any[], options?: QueryOptions): Promise<any>;
}