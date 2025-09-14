import { BaseAdapter } from './adapters/BaseAdapter';
export interface DatabaseConnection {
    [key: string]: any;
}
export declare class UniversalORM {
    private connections;
    private factory;
    constructor();
    /**
     * Create a new database connection
     * @param type - Database type (e.g., 'postgres', 'mongodb')
     * @param config - Connection configuration
     * @returns Database instance
     */
    connect(type: string, config: Record<string, any>): BaseAdapter;
    /**
     * Close all database connections
     */
    closeAll(): Promise<void>;
    /**
     * Get all active connections
     */
    getConnections(): Map<string, BaseAdapter>;
}
declare const db: UniversalORM;
export default db;
export { BaseAdapter };
export * as adapters from './adapters';
//# sourceMappingURL=index.d.ts.map