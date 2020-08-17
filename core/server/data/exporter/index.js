const _ = require('lodash');
const Promise = require('bluebird');
const db = require('../../data/db');
const commands = require('../schema').commands;
const ghostVersion = require('../../lib/ghost-version');
const {i18n} = require('../../lib/common');
const logging = require('../../../shared/logging');
const errors = require('@tryghost/errors');
const security = require('../../lib/security');
const models = require('../../models');
const EXCLUDED_TABLES = ['sessions', 'mobiledoc_revisions'];
const stream = require('stream');

const modelOptions = {context: {internal: true}};

// private
let getVersionAndTables;

let exportTable;

// public
let doExport;

let exportFileName;

exportFileName = function exportFileName(options) {
    const datetime = require('moment')().format('YYYY-MM-DD-HH-mm-ss');
    let title = '';

    options = options || {};

    // custom filename
    if (options.filename) {
        return Promise.resolve(options.filename + '.json');
    }

    return models.Settings.findOne({key: 'title'}, _.merge({}, modelOptions, _.pick(options, 'transacting'))).then(function (result) {
        if (result) {
            title = security.string.safe(result.get('value')) + '.';
        }

        return title + 'ghost.' + datetime + '.json';
    }).catch(function (err) {
        logging.error(new errors.GhostError({err: err}));
        return 'ghost.' + datetime + '.json';
    });
};

getVersionAndTables = function getVersionAndTables(options) {
    const props = {
        version: ghostVersion.full,
        tables: commands.getTables(options.transacting)
    };

    return Promise.props(props);
};

exportTable = function exportTable(tableName, options) {
    if (EXCLUDED_TABLES.indexOf(tableName) < 0 ||
        (options.include && _.isArray(options.include) && options.include.indexOf(tableName) !== -1)) {
        const query = (options.transacting || db.knex)(tableName);

        // TODO: return a readable stream
        // See http://knexjs.org/#Interfaces-Streams
        return query.select().stream();
    }
};

function joinedStream(...streams) {
    function pipeNext() {
        const nextStream = streams.shift();
        if (nextStream) {
            nextStream.pipe(out, {end: false});
            nextStream.on('end', function () {
                pipeNext();
            });
        } else {
            out.end();
        }
    }
    const out = new stream.PassThrough();
    pipeNext();
    return out;
}

async function * generateHeader(version) {
    yield `{
    "meta": {
        "exported_on": ${new Date().getTime()},
        "version": "${version}"
    },
    "data": {`;
}

async function * generateDataBlob(tableName, tableData) {
    yield `
        "${tableName}": [`;
    
    if (tableData !== undefined) {
        let needsComma = false;
        for await (const chunk of tableData) {
            // logging.info(JSON.stringify(chunk));
            if (needsComma) {
                yield `,`;
            }
            yield JSON.stringify(chunk);
            needsComma = true;
        }
    } else {
        yield ` `;
    }
    yield `],`;
}

async function * generateFooter() {
    yield `
    }
};`;
}

doExport = function doExport(options) {
    options = options || {include: []};

    let tables;
    let version;

    return getVersionAndTables(options).then(function exportAllTables(result) {
        tables = result.tables;
        version = result.version;

        return tables.map(function (tableName) {
            return exportTable(tableName, options);
        });
    }).then(function formatData(tableData) {
        // stream in header
        const headerStream = new stream.Readable.from(generateHeader(version));
        
        const tableStreams = tables.map(function (name, i) {
            return new stream.Readable.from(generateDataBlob(name, tableData[i]));
        });

        // stream in footer
        const footerStream = new stream.Readable.from(generateFooter());

        return joinedStream(headerStream, ...tableStreams, footerStream);
    }).catch(function (err) {
        return Promise.reject(new errors.DataExportError({
            err: err,
            context: i18n.t('errors.data.export.errorExportingData')
        }));
    });
};

module.exports = {
    doExport: doExport,
    fileName: exportFileName,
    EXCLUDED_TABLES: EXCLUDED_TABLES
};
