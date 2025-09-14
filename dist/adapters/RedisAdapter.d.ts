import { BaseAdapter, DatabaseConfig, QueryOptions, CreateOptions, UpdateOptions, DeleteOptions, TransactionOptions, ModelSchema, BulkUpdateOperation } from './BaseAdapter';
export default class RedisAdapter extends BaseAdapter {
    constructor(config: DatabaseConfig);
    initialize(options?: Record<string, any>): Promise<void>;
    create(model: string, data: Record<string, any> | Record<string, any>[], options?: CreateOptions): Promise<Record<string, any> | Record<string, any>[]>;
    findOne(model: string, query: Record<string, any>, options?: QueryOptions): Promise<Record<string, any> | null>;
    findMany(model: string, query?: Record<string, any>, options?: QueryOptions): Promise<Record<string, any>[]>;
    update(model: string, query: Record<string, any>, data: Record<string, any>, options?: UpdateOptions): Promise<Record<string, any>>;
    delete(model: string, query: Record<string, any>, options?: DeleteOptions): Promise<Record<string, any>>;
    transaction<T>(callback: () => Promise<T>, options?: TransactionOptions): Promise<T>;
    close(): Promise<void>;
    createModel(model: string, schema: ModelSchema, options?: Record<string, any>): Promise<void>;
    dropModel(model: string, options?: Record<string, any>): Promise<void>;
    count(model: string, query?: Record<string, any>, options?: QueryOptions): Promise<number>;
    exists(model: string, query: Record<string, any>, options?: QueryOptions): Promise<boolean>;
    bulkCreate(model: string, data: Record<string, any>[], options?: CreateOptions): Promise<Record<string, any>>;
    bulkUpdate(model: string, updates: BulkUpdateOperation[], options?: UpdateOptions): Promise<Record<string, any>>;
    aggregate(model: string, pipeline: Record<string, any>[] | Record<string, any>, options?: QueryOptions): Promise<Record<string, any>[]>;
    query(query: string | Record<string, any>, params?: Record<string, any> | any[], options?: QueryOptions): Promise<any>;
}
//# sourceMappingURL=RedisAdapter.d.ts.map