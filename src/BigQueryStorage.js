/**
 * @author David Menger
 */
'use strict';

const isSubset = require('is-subset');
const deepExtend = require('deep-extend');
const { BigQuery } = require('@google-cloud/bigquery');
const { TrackingType } = require('wingbot');

/** @typedef {import('@google-cloud/bigquery').BigQueryOptions['credentials']} Credentials */
/** @typedef {import('@google-cloud/bigquery').TableMetadata} TableMetadata */
/** @typedef {import('@google-cloud/bigquery').TableSchema} TableSchema */

/** @typedef {import('wingbot/src/analytics/onInteractionHandler').IAnalyticsStorage} IAnalyticsStorage */ // eslint-disable-line max-len
/** @typedef {import('wingbot/src/analytics/onInteractionHandler').IGALogger} IGALogger */
/** @typedef {import('wingbot/src/analytics/onInteractionHandler').SessionMetadata} SessionMetadata */ // eslint-disable-line max-len
/** @typedef {import('wingbot/src/analytics/onInteractionHandler').GAUser} GAUser */
/** @typedef {import('wingbot/src/analytics/onInteractionHandler').TrackingEvent} Event */

/* eslint object-curly-newline: 0 */

/** @type {TableSchema} */
const EVENTS_SCHEMA = {
    fields: [
        { name: 'pageId', type: 'STRING', maxLength: '36', mode: 'REQUIRED' }, // uuid length
        { name: 'senderId', type: 'STRING', maxLength: '36', mode: 'REQUIRED' },
        { name: 'sessionId', type: 'STRING', maxLength: '32', mode: 'REQUIRED' },

        { name: 'timestamp', type: 'TIMESTAMP', mode: 'REQUIRED' },
        { name: 'datetime', type: 'DATETIME', mode: 'REQUIRED' },
        { name: 'date', type: 'DATE', mode: 'REQUIRED' },

        { name: 'category', type: 'STRING', maxLength: '3', mode: 'REQUIRED' },

        { name: 'type', type: 'STRING', maxLength: '12', mode: 'REQUIRED' },
        { name: 'action', type: 'STRING' },
        { name: 'label', type: 'STRING' },
        { name: 'value', type: 'INTEGER' },
        { name: 'lang', type: 'STRING', maxLength: '2' },

        { name: 'nonInteractive', type: 'BOOLEAN', mode: 'REQUIRED' }
    ]
};

/** @type {TableSchema} */
const SESSIONS_SCHEMA = {
    fields: [
        { name: 'pageId', type: 'STRING', maxLength: '36', mode: 'REQUIRED' }, // uuid length
        { name: 'senderId', type: 'STRING', maxLength: '36', mode: 'REQUIRED' },
        { name: 'sessionId', type: 'STRING', maxLength: '32', mode: 'REQUIRED' },

        { name: 'sessionStart', type: 'TIMESTAMP', mode: 'REQUIRED' },
        { name: 'sessionStartDate', type: 'DATETIME', mode: 'REQUIRED' },
        { name: 'date', type: 'DATE', mode: 'REQUIRED' },

        { name: 'action', type: 'STRING' },
        { name: 'sessionCount', type: 'INTEGER' },
        { name: 'lang', type: 'STRING', maxLength: '2' },
        { name: 'nonInteractive', type: 'BOOLEAN', mode: 'REQUIRED' },

        { name: 'botId', type: 'STRING', maxLength: '36' }, // uuid length
        { name: 'snapshot', type: 'STRING', maxLength: '14' }
    ]
};

/** @type {TableSchema} */
const CONVERSATIONS_SCHEMA = {
    fields: [
        { name: 'pageId', type: 'STRING', maxLength: '36', mode: 'REQUIRED' }, // uuid length
        { name: 'senderId', type: 'STRING', maxLength: '36', mode: 'REQUIRED' },
        { name: 'sessionId', type: 'STRING', maxLength: '32', mode: 'REQUIRED' },

        { name: 'timestamp', type: 'TIMESTAMP', mode: 'REQUIRED' },
        { name: 'datetime', type: 'DATETIME', mode: 'REQUIRED' },
        { name: 'date', type: 'DATE', mode: 'REQUIRED' },

        { name: 'category', type: 'STRING', maxLength: '3', mode: 'REQUIRED' },

        { name: 'action', type: 'STRING' },
        { name: 'lastAction', type: 'STRING' },
        { name: 'label', type: 'STRING' },
        { name: 'value', type: 'INTEGER' },
        { name: 'lang', type: 'STRING', maxLength: '2' },
        { name: 'skill', type: 'STRING' },

        { name: 'text', type: 'STRING' },

        { name: 'expected', type: 'STRING' },

        { name: 'expectedTaken', type: 'BOOLEAN', mode: 'REQUIRED' },
        { name: 'isContextUpdate', type: 'BOOLEAN', mode: 'REQUIRED' },
        { name: 'isAttachment', type: 'BOOLEAN', mode: 'REQUIRED' },
        { name: 'isNotification', type: 'BOOLEAN', mode: 'REQUIRED' },
        { name: 'isQuickReply', type: 'BOOLEAN', mode: 'REQUIRED' },
        { name: 'isPassThread', type: 'BOOLEAN', mode: 'REQUIRED' },
        { name: 'isText', type: 'BOOLEAN', mode: 'REQUIRED' },
        { name: 'isPostback', type: 'BOOLEAN', mode: 'REQUIRED' },
        { name: 'didHandover', type: 'BOOLEAN', mode: 'REQUIRED' },

        { name: 'withUser', type: 'BOOLEAN', mode: 'REQUIRED' },
        { name: 'userId', type: 'STRING' },
        { name: 'feedback', type: 'INTEGER', mode: 'REQUIRED' },

        { name: 'sessionStart', type: 'TIMESTAMP', mode: 'REQUIRED' },
        { name: 'sessionStartDate', type: 'DATETIME', mode: 'REQUIRED' },
        { name: 'sessionDuration', type: 'INTEGER', mode: 'REQUIRED' },

        { name: 'winnerAction', type: 'STRING' },
        { name: 'winnerIntent', type: 'STRING' },
        { name: 'winnerEntities', type: 'STRING', mode: 'REPEATED' },
        { name: 'winnerScore', type: 'FLOAT' },
        { name: 'winnerTaken', type: 'BOOLEAN' },

        { name: 'intent', type: 'STRING' },
        { name: 'intentScore', type: 'FLOAT' },
        { name: 'entities', type: 'STRING', mode: 'REPEATED' },

        { name: 'allActions', type: 'STRING', mode: 'REPEATED' },

        { name: 'nonInteractive', type: 'BOOLEAN', mode: 'REQUIRED' },

        { name: 'botId', type: 'STRING', maxLength: '36' }, // uuid length
        { name: 'snapshot', type: 'STRING', maxLength: '14' }
    ]
};

/**
 * @typedef {object} Logger
 * @prop {Function} log
 * @prop {Function} error
 */

/**
 * @class {BigQueryStorage}
 * @implements IAnalyticsStorage
 */
class BigQueryStorage {

    /**
     *
     * @param {Credentials} googleCredentials
     * @param {string} projectId
     * @param {string} dataset
     * @param {object} [options]
     * @param {Logger} [options.log]
     * @param {boolean} [options.throwExceptions]
     * @param {boolean} [options.passiveSchema] - disables automatic topology updates
     */
    constructor (googleCredentials, projectId, dataset, options = {}) {
        this._client = new BigQuery({
            credentials: googleCredentials,
            projectId
        });

        this._db = this._client.dataset(dataset);

        this._log = options.log || console;
        this._passiveSchema = !!options.passiveSchema;
        this._throwExceptions = !!options.throwExceptions;

        this._schemaUpdated = null;

        this.SESSIONS = 'sessions';
        this.EVENTS = 'events';
        this.CONVERSATIONS = 'conversations';

        this.hasExtendedEvents = true;
        this.supportsArrays = true;
        this.useDescriptiveCategories = false;
        this.useExtendedScalars = true;
        this.parallelSessionInsert = true;
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
     * @param {TableMetadata} definition
     * @returns {Promise}
     */
    async _upsertTable (definition) {
        let exists = false;
        try {
            const { name, ...metadata } = definition;
            const table = this._db.table(name);
            [exists] = await table.exists();

            if (!exists) {
                this._log.log(`BigQueryStorage: creating table ${name}...`);
                await table.create(metadata);
                this._log.log(`BigQueryStorage: table ${name} created`);
                // await this._db.createTable(name, metadata);
                return;
            }

            const [md] = await table.getMetadata();

            if (isSubset(md, metadata)) {
                this._log.log(`BigQueryStorage: table ${name} is up to date`);
                return;
            }

            deepExtend(md, metadata);
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
        await Promise.all([
            this._upsertTable({
                name: this.EVENTS,
                schema: EVENTS_SCHEMA,
                timePartitioning: {
                    type: 'DAY', // 'MONTH'
                    expirationMs: '3118560000000', // 10 years
                    field: 'date'
                },
                clustering: {
                    fields: ['pageId', 'sessionId']
                }
            }),
            this._upsertTable({
                name: this.CONVERSATIONS,
                schema: CONVERSATIONS_SCHEMA,
                timePartitioning: {
                    type: 'DAY', // 'MONTH'
                    expirationMs: '3118560000000', // 10 years
                    field: 'date'
                },
                clustering: {
                    fields: ['pageId', 'sessionId']
                }
            }),
            this._upsertTable({
                name: this.SESSIONS,
                schema: SESSIONS_SCHEMA,
                timePartitioning: {
                    type: 'DAY', // 'MONTH'
                    expirationMs: '3118560000000', // 10 years
                    field: 'date'
                },
                clustering: {
                    fields: ['pageId', 'sessionId']
                }
            })
        ]);
        if (this._throwExceptions) {
            this._log.log(`BigQueryStorage: topology\t${Date.now() - d}`);
        }
        return true;
    }

    /**
     *
     * @param {string} pageId
     * @param {string} senderId
     * @param {string} sessionId
     * @param {SessionMetadata} [metadata]
     * @param {number} [ts]
     * @param {boolean} [nonInteractive]
     * @param {string} [timeZone]
     * @returns {Promise}
     */
    async createUserSession (
        pageId,
        senderId,
        sessionId,
        metadata,
        ts = Date.now(),
        nonInteractive = false,
        timeZone = 'UTC'
    ) {
        const {
            sessionCount,
            botId,
            snapshot,
            lang,
            action
        } = metadata;

        await this._insert(this.SESSIONS, [
            {
                pageId,
                senderId,
                sessionId,
                sessionStart: this._dateTime(ts),
                sessionStartDate: this._dateTime(ts, timeZone),
                date: this._date(ts, timeZone),
                action,
                sessionCount,
                nonInteractive,
                lang,
                botId,
                snapshot
            }
        ]);
    }

    _nullable (val) {
        return val || null;
    }

    /**
     *
     * @param {string} pageId
     * @param {string} senderId
     * @param {string} sessionId
     * @param {Event[]} trackingEvents
     * @param {GAUser} [user]
     * @param {number} [ts]
     * @param {boolean} [nonInteractive]
     * @param {boolean} [sessionStarted]
     * @param {string} [timeZone]
     * @returns {Promise}
     */
    async storeEvents (
        pageId,
        senderId,
        sessionId,
        trackingEvents,
        user = null,
        ts = Date.now(),
        nonInteractive = false,
        sessionStarted = false, // eslint-disable-line no-unused-vars
        timeZone = 'UTC'
    ) {
        const conversations = [];
        const events = trackingEvents
            .filter((e) => {
                if (e.type === TrackingType.CONVERSATION_EVENT && 'allActions' in e) {
                    conversations.push({
                        pageId,
                        senderId,
                        sessionId,
                        timestamp: this._dateTime(ts),
                        datetime: this._dateTime(ts, timeZone),
                        date: this._date(ts, timeZone),
                        category: e.category,
                        action: this._nullable(e.action),
                        label: this._nullable(e.label),
                        value: this._nullable(e.value),
                        lang: this._nullable(e.lang),
                        skill: this._nullable(e.skill),

                        text: this._nullable(e.text),
                        expected: this._nullable(e.expected),

                        expectedTaken: e.expectedTaken,
                        isContextUpdate: e.isContextUpdate,
                        isAttachment: e.isAttachment,
                        isNotification: e.isNotification,
                        isQuickReply: e.isQuickReply,
                        isPassThread: e.isPassThread,
                        isPostback: e.isPostback,
                        isText: e.isText,
                        didHandover: e.didHandover,
                        withUser: e.withUser,

                        userId: this._nullable(user && user.id),
                        feedback: e.feedback,

                        sessionStart: this._dateTime(e.sessionStart),
                        sessionStartDate: this._dateTime(e.sessionStart, timeZone),
                        sessionDuration: e.sessionDuration,

                        winnerAction: this._nullable(e.winnerAction),
                        winnerIntent: this._nullable(e.winnerIntent),
                        winnerEntities: e.winnerEntities,
                        winnerScore: e.winnerScore,
                        winnerTaken: e.winnerTaken,

                        intent: this._nullable(e.intent),
                        intentScore: this._nullable(e.intentScore),
                        entities: e.entities,

                        allActions: e.allActions,

                        nonInteractive,

                        snapshot: this._nullable(e.snapshot),
                        botId: this._nullable(e.botId)
                    });
                    return false;
                }
                return true;
            })
            .map((e) => ({
                pageId,
                senderId,
                sessionId,
                timestamp: this._dateTime(ts),
                datetime: this._dateTime(ts, timeZone),
                date: this._date(ts, timeZone),
                type: e.type,
                category: e.category,
                action: this._nullable(e.action),
                label: this._nullable(e.label),
                value: this._nullable(e.value),
                lang: this._nullable(e.lang),
                nonInteractive
            }));

        const d = Date.now();
        await Promise.all([
            this._insert(this.CONVERSATIONS, conversations),
            this._insert(this.EVENTS, events)
        ]);
        if (this._throwExceptions) {
            this._log.log(`BigQueryStorage: inserts\t${Date.now() - d}`);
        }
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

module.exports = BigQueryStorage;
