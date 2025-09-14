import { BaseAdapter, DatabaseConfig, QueryOptions, CreateOptions, UpdateOptions, DeleteOptions, TransactionOptions, ModelSchema, BulkUpdateOperation } from './BaseAdapter';
interface MongoQueryOptions extends QueryOptions {
    select?: string | string[];
    populate?: string | string[];
}
interface MongoUpdateOptions extends UpdateOptions {
    new?: boolean;
}
export default class MongoAdapter extends BaseAdapter {
    private models;
    private connection;
    constructor(config: DatabaseConfig);
    /**
     * Initialize MongoDB connection
     */
    initialize(options?: Record<string, any>): Promise<void>;
    /**
     * Create a new record in the database
     */
    create(model: string, data: Record<string, any> | Record<string, any>[], options?: CreateOptions): Promise<Record<string, any> | Record<string, any>[]>;
    /**
     * Find a single record
     */
    findOne(model: string, query: Record<string, any>, options?: MongoQueryOptions): Promise<Record<string, any> | null>;
    /**
     * Find multiple records
     */
    findMany(model: string, query?: Record<string, any>, options?: MongoQueryOptions): Promise<Record<string, any>[]>;
    /**
     * Update records
     */
    update(model: string, query: Record<string, any>, data: Record<string, any>, options?: MongoUpdateOptions): Promise<Record<string, any>>;
    /**
     * Delete records
     */
    delete(model: string, query: Record<string, any>, options?: DeleteOptions): Promise<Record<string, any>>;
    /**
     * Execute operations in a transaction
     */
    transaction<T>(callback: () => Promise<T>, options?: TransactionOptions): Promise<T>;
    /**
     * Close database connection
     */
    close(): Promise<void>;
    /**
     * Create a new model/collection/table
     */
    createModel(model: string, schema: ModelSchema, options?: Record<string, any>): Promise<void>;
    /**
     * Drop a model/collection/table
     */
    dropModel(model: string, options?: Record<string, any>): Promise<void>;
    /**
     * Count records
     */
    count(model: string, query?: Record<string, any>, options?: QueryOptions): Promise<number>;
    /**
     * Check if records exist
     */
    exists(model: string, query: Record<string, any>, options?: QueryOptions): Promise<boolean>;
    /**
     * Bulk insert records
     */
    bulkCreate(model: string, data: Record<string, any>[], options?: CreateOptions): Promise<Record<string, any>>;
    /**
     * Bulk update records
     */
    bulkUpdate(model: string, updates: BulkUpdateOperation[], options?: UpdateOptions): Promise<Record<string, any>>;
    /**
     * Perform aggregation
     */
    aggregate(model: string, pipeline: Record<string, any>[] | Record<string, any>, options?: QueryOptions): Promise<Record<string, any>[]>;
    /**
     * Execute raw query
     */
    query(query: string | Record<string, any>, params?: Record<string, any> | any[], options?: QueryOptions): Promise<any>;
    private _getModel;
    private _convertSchema;
    private _convertFieldType;
    private _convertQuery;
    private _processQueryOperators;
    private _convertUpdateOperations;
    private _addSchemaIndexes;
}
export {};
//# sourceMappingURL=MongoAdapter.d.ts.map