/**
 * @author David Menger
 */
'use strict';

const isSubset = require('is-subset');
const deepExtend = require('deep-extend');
const { BigQuery } = require('@google-cloud/bigquery');

/** @typedef {import('@google-cloud/bigquery').BigQueryOptions['credentials']} Credentials */
/** @typedef {import('@google-cloud/bigquery').TableMetadata} TableMetadata */

/** @typedef {import('wingbot/src/analytics/onInteractionHandler').IGALogger} IGALogger */
/** @typedef {import('@google-cloud/bigquery').Table} Table */

/* eslint object-curly-newline: 0 */

/**
 * @typedef {object} Logger
 * @prop {Function} log
 * @prop {Function} error
 */

/**
 * @class {BaseBigQueryStorage}
 */
class BaseBigQueryStorage {

    /**
     *
     * @param {Credentials} googleCredentials
     * @param {string} projectId
     * @param {string} dataset
     * @param {TableMetadata[]} topology
     * @param {object} [options]
     * @param {Logger} [options.log]
     * @param {boolean} [options.throwExceptions]
     * @param {boolean} [options.passiveSchema] - disables automatic topology updates
     */
    constructor (googleCredentials, projectId, dataset, topology, options = {}) {
        this._client = new BigQuery({
            credentials: googleCredentials,
            projectId
        });

        this._projectId = projectId;
        this._dataset = dataset;
        this._db = this._client.dataset(dataset);

        this._log = options.log || console;
        this._passiveSchema = !!options.passiveSchema;
        this._throwExceptions = !!options.throwExceptions;

        this._schemaUpdated = null;

        this.hasExtendedEvents = true;
        this.supportsArrays = true;
        this.useDescriptiveCategories = false;
        this.useExtendedScalars = true;
        this.parallelSessionInsert = true;

        /** @type {TableMetadata[]} */
        this.topology = topology;

    }

    preHeat () {
        return this.db();
    }

    async db () {
        if (this._schemaUpdated === true || this._passiveSchema) {
            return this._db;
        }
        if (!this._schemaUpdated) {
            this._schemaUpdated = this._updateTopology();
        }
        try {
            await this._schemaUpdated;
            this._schemaUpdated = true;
        } catch (e) {
            this._schemaUpdated = null;
            this._log.error('BigQueryStorage: failed to create/update topology', e);
            throw e;
        }
        return this._db;
    }

    /**
     * @param {IGALogger} logger
     */
    setDefaultLogger (logger) {
        if (this._log === console) {
            this._logger = logger;
        }
    }

    /**
     * get date like "YYYY-MM-DD"
     *
     * @param {number} timestamp
     * @param {string} timeZone
     * @returns {string}
     */
    _date (timestamp, timeZone = 'UTC') {
        return new Date(timestamp)
            .toLocaleDateString('sv-SE', { timeZone });
    }

    /**
     * get date like "YYYY-MM-DD HH:II:SS"
     *
     * @param {number} timestamp
     * @param {string} timeZone
     * @returns {string}
     */
    _dateTime (timestamp, timeZone = 'UTC') {
        return new Date(timestamp)
            .toLocaleString('sv-SE', { timeZone });
    }

    /**
     *
     * @param {Table[]} tables
     * @param {TableMetadata} definition
     * @returns {Promise}
     */
    async _upsertTable (tables, definition) {
        let exists = false;
        try {
            const { name, ...metadata } = definition;
            const table = this._db.table(name);

            exists = tables.some((t) => t.id === name);

            if (definition.materializedView) {
                const sourceTableExists = tables.some((t) => t.id === metadata.description);

                if (!sourceTableExists) {
                    this._log.log(`BigQueryStorage: view "${name}" will be inserted later, because the source table "${metadata.description}" doesn't exist.`);
                    return;
                }
            }

            if (!exists) {
                this._log.log(`BigQueryStorage: creating table ${name}...`);
                await table.create(metadata);
                this._log.log(`BigQueryStorage: table ${name} created`);
                return;
            }

            const [md] = await table.getMetadata();

            if (isSubset(md, metadata)) {
                this._log.log(`BigQueryStorage: table ${name} is up to date`);
                return;
            }

            deepExtend(md, metadata);

            if (definition.materializedView) {
                this._log.log(`BigQueryStorage: removing view ${name}...`);
                await table.delete({ ignoreNotFound: true });

                this._log.log(`BigQueryStorage: creating view ${name} again...`);
                await table.create(metadata);
                this._log.log(`BigQueryStorage: view ${name} updated`);
                return;
            }

            this._log.log(`BigQueryStorage: updating table ${name}...`);
            await table.setMetadata(md);
            this._log.log(`BigQueryStorage: table ${name} updated`);
        } catch (e) {
            this._log.error('BigQueryStorage: failed to update topology', e);
            if (!exists || this._throwExceptions) {
                throw e;
            }
        }
    }

    async _updateTopology () {
        const d = Date.now();

        const [tables] = await this._db.getTables();

        await Promise.all(
            this.topology.map((t) => this._upsertTable(tables, t))
        );
        if (this._throwExceptions) {
            this._log.log(`BigQueryStorage: topology\t${Date.now() - d}`);
        }
        return true;
    }

    /**
     *
     * @param {string} table
     * @param {object[]} data
     */
    async _insert (table, data) {
        if (data.length === 0) {
            return;
        }
        try {
            const db = await this.db();
            await db.table(table).insert(data);
        } catch (e) {
            let details = null;
            if (e.response && e.response.insertErrors) {
                details = e.response.insertErrors.flatMap((er) => er.errors || []);
            }
            this._log.error(`BigQueryStorage: insert to "${table}" failed`, e, details);
            if (this._throwExceptions) {
                throw e;
            }
        }
    }
}

module.exports = BaseBigQueryStorage;
