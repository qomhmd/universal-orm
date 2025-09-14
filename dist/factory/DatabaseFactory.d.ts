import { BaseAdapter } from '../adapters/BaseAdapter';
export declare class DatabaseFactory {
    private adapters;
    constructor();
    /**
     * Create database instance based on type
     * @param type - Database type
     * @param config - Connection configuration
     * @returns Database adapter instance
     */
    createDatabase(type: string, config: Record<string, any>): BaseAdapter;
    /**
     * Get list of supported databases
     * @returns List of supported database types
     */
    getSupportedDatabases(): string[];
    /**
     * Check if database type is supported
     * @param type - Database type to check
     * @returns Whether the database type is supported
     */
    isSupported(type: string): boolean;
    /**
     * Add custom adapter
     * @param type - Database type
     * @param AdapterClass - Adapter class implementation
     */
    addAdapter(type: string, AdapterClass: new (config: Record<string, any>) => BaseAdapter): void;
    /**
     * Remove adapter
     * @param type - Database type to remove
     */
    removeAdapter(type: string): void;
}
//# sourceMappingURL=DatabaseFactory.d.ts.map