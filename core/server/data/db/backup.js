// # Backup Database
// Provides for backing up the database before making potentially destructive changes
const fs = require('fs-extra');

const path = require('path');
const Promise = require('bluebird');
const config = require('../../../shared/config');
const logging = require('../../../shared/logging');
const urlUtils = require('../../../shared/url-utils');
const exporter = require('../exporter');
let writeExportFile;
let backup;

writeExportFile = async function writeExportFile(exportResult) {
    const { data, filename } = exportResult;
    const resolvedFilename = path.resolve(urlUtils.urlJoin(config.get('paths').contentPath, 'data', exportResult.filename));

    const writeStream = fs.createWriteStream(resolvedFilename);

    const pipe = data.pipe(writeStream);

    pipe.on('close', function () {
        logging.info('All done!');
    });
    pipe.on('finish', function () {
        logging.info('Finish All done!');
    });
    data.on('close', function () {
        logging.info('All done!');
    });
    data.on('finish', function () {
        logging.info('Finish All done!');
    });

    // return fs.writeFile(filename, JSON.stringify(exportResult.data)).return(filename);
    return resolvedFilename;
};

const readBackup = async (filename) => {
    const parsedFileName = path.parse(filename);
    const sanitized = `${parsedFileName.name}${parsedFileName.ext}`;
    const backupPath = path.resolve(urlUtils.urlJoin(config.get('paths').contentPath, 'data', sanitized));

    const exists = await fs.pathExists(backupPath);

    if (exists) {
        const backup = await fs.readFile(backupPath);
        return JSON.parse(backup);
    } else {
        return null;
    }
};

/**
 * ## Backup
 * does an export, and stores this in a local file
 * @returns {Promise<*>}
 */
backup = function backup(options) {
    logging.info('Creating database backup');
    options = options || {};

    // Pipes, not promises
    
    const props = {
        data: exporter.doExport(options),
        filename: exporter.fileName(options)
    };

    return Promise.props(props)
        .then(writeExportFile)
        .then(function successMessage(filename) {
            logging.info('Database backup written to: ' + filename);
            return filename;
        });
};

module.exports = {
    backup,
    readBackup
};
