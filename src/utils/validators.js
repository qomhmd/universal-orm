class ConfigValidator {
    static validate(type, config) {
        if (!type || typeof type !== 'string') {
            throw new Error('Database type must be a non-empty string');
        }

        if (!config || typeof config !== 'object') {
            throw new Error('Configuration must be an object');
        }

        // Basic configuration requirements
        const requiredFields = {
            postgres: ['host', 'port', 'database', 'user', 'password'],
            mongodb: ['uri', 'database'],
            redis: ['host', 'port'],
            // ... other database types
        };

        const fields = requiredFields[type.toLowerCase()];
        if (fields) {
            for (const field of fields) {
                if (!config[field]) {
                    throw new Error(`Missing required field: ${field}`);
                }
            }
        }
    }
}

module.exports = { ConfigValidator };

