const axios = require('axios'),
    csvWriter = require('csv-write-stream'),
    fs = require('fs'),
    moment = require('moment'),
    nodemailer = require('nodemailer'),
    path = require('path'),
    util = require('util'),
    https = require('https');

const exec = util.promisify(require('child_process').exec);

global.storageFilePath = '/var/www/stasis-calllogs/';
global.failedStorageFilePath = '/var/www/stasis-calllogs/calllogs-failed/';
global.logStorageFilePath = '/var/log/stasis/';
global.safeShutdown = false;
global.appPath = __dirname;


async function processCallReport() {

    checkSafeShutdown();
    if (global.safeShutdown) {
        console.log('Remove safe-shutdown file to start the application: configs/safe-shutdown');
        process.exit();
    }
    while (1) {
        try {
            let dirents = await fs.promises.readdir(storageFilePath, {
                withFileTypes: true
            });

            let validFileMtime = new Date().getTime() - (5 * 1000);
            let files = dirents.map(dirent => {
                    dirent.mtime = fs.lstatSync(storageFilePath + dirent.name).mtime;
                    return dirent;
                })
                .filter(dirent => dirent.isFile() && !(/^\./).test(dirent.name) && validFileMtime > new Date(dirent.mtime).getTime())
                .sort((a, b) => a.mtime.getTime() - b.mtime.getTime())
                .map(dirent => dirent.name);

            const chunkSize = 20;
            const chunks = [];
            while (files.length) {
                chunks.push(files.splice(0, chunkSize));
            }

            for (const chunk of chunks) {
                await Promise.all(chunk.map(async (file) => {
                    var fi = storageFilePath + file;
                    var message = "filename:" + file;
                    // global.fileName = file;
                    try {
                        let data = await fs.promises.readFile(fi, {
                            encoding: 'utf-8'
                        });
                        if(data) {
                            var obj = JSON.parse(data);
                            const timeStart = Date.now();
                            let result;
                            if (checkFirstTry(obj) || checkRetry(obj)) {
                                logger(message, file);
                                logger(data, file);
                                if (obj.callback) {
                                    result = await pushCallDetails(obj, file);
                                } else if (obj.mongo) {
                                    result = await writeCalllogs(obj, file);
                                }
                                if (result) {
                                    removeFiles(file);
                                } else {
                                    await setRetryTime(obj, file);
                                }
                                message = 'Total execution time in milliseconds: ' + (Date.now() - timeStart);
                                logger(message, file);
                            }
                        } else {
                            try {
                                var subject = 'Asterisk Call Handler Empty File - ' + file;
                                message = '</h5><h5>File Name : ' + file;
                                logger(subject, file);
                                sendEmail(subject, message);
                                await fs.promises.rename(storageFilePath + file, failedStorageFilePath + file);
                            } catch (err) {
                                message = "Error file Moving error: " + err;
                                console.error(file + '; ' + message);
                                logger(message, file);
                            }
                        }
                    } catch (err) {
                        message = "Processing file content error: " + err;
                        console.error(file + '; ' + message);
                        logger(message, file);
                        try {
                            await fs.promises.rename(storageFilePath + file, failedStorageFilePath + file);
                        } catch (err) {
                            message = "Error file Moving error: " + err;
                            console.error(file + '; ' + message);
                            logger(message, file);
                        }
                    }
                }));
                logger('Complete execution of a chunk', 'INFO');
                if (global.safeShutdown) {
                    logger('Breaking batch loop: safeShutdown flag found', 'INFO');
                    break;
                }
            }
            logger('Complete execution of one batch', 'INFO');
            if (global.safeShutdown) {
                logger('Breaking main loop; safeShutdown flag found; Shutting down', 'INFO');
                process.exit();
            }
            await sleep(1000);
        } catch (e) {
            console.log('<<ERROR>>');
            console.error(e);
            logger('Something is not good: ' + e, 'ERROR');
            await sleep(20 * 1000);
        }
    }
}

const sleep = util.promisify(setTimeout);

function checkFirstTry(data) {
    if (data._RetryTime && data._TryCount) {
        return false;
    } else {
        return true;
    }
}

function checkRetry(data) {
    if (data._RetryTime) {
        if (data._RetryTime <= moment().format('YYYY-MM-DD HH:mm:ss') && data._TryCount <= 50) {
            return true;
        } else if (data._TryCount >= 50) {
            return true;
        } else {
            return false;
        }
    }
}


async function pushCallDetails(data, file) {
    var params = {
        'url': data.callback.url,
        'requestType': data.callback.method
    };

    if (data.callback.headers && data.callback.headers.user) {
        params.user = data.callback.headers.user;
    }

    if (data.callback.headers && data.callback.headers.authorization) {
        params.authorization = data.callback.headers.authorization;
    }

    if (data.callback.headers && data.callback.headers['Content-Type']) {
        params['Content-Type'] = data.callback.headers['Content-Type'];
    }

    if (data.callback.headers && data.callback.headers.httpHeaders) {
        params.httpHeaders = data.callback.headers.httpHeaders;
    }


    var response = await triggerCallback(params, data, file);
    var msg, res;

    if (!(response.httpcode >= 200 && response.httpcode < 300)) {
        msg = 'API Failure Response';
        logger(msg, file);
        res = JSON.stringify(response);
        logger(res, file);
        if (data._TryCount && data._TryCount % 10 == 0 || data._TryCount > 50 ) {
            var subject = 'Asterisk Call Handler Data push failed - ' + data.AppName;
            var message = '<h5>RetryTime :' + data._RetryTime + '</h5><h5>Try Count : ' + data._TryCount + '</h5><h5>Method : Axios</h5><h5>Host : ' +
                data.callback.url + '</h5><h5>Calllog : ' + JSON.stringify(data.Data) + '</h5><h5>Response : ' + res + '</h5><h5>File Name : ' + file;
            sendEmail(subject, message);

        }
        return false;
    } else {
        msg = "API Push Response ";
        logger(msg, file);
        res = JSON.stringify(response);
        logger(res, file);
        successLog(data, response);
        return true;
    }
}

async function triggerCallback(conf, data, file) {
    let apiResponse = {};
    if (conf.url) {
        if (data) {
            var header = {};

            if (conf.httpHeaders) {
                header = conf.httpHeaders;
            }

            if (conf.authorization) {
                header.Authorization = conf.authorization;
            }

            if (conf['Content-Type']) {
                header['Content-Type'] = conf['Content-Type'];
            }

            if (conf.user) {
                const encodedToken = Buffer.from(conf.user).toString('base64');
                header.Authorization = 'Basic ' + encodedToken;
            }

            const agent = new https.Agent({
                rejectUnauthorized: false,
            });

            var config = {
                method: conf.requestType,
                url: conf.url,
                headers: header,
                data: data.Data,
                timeout: 10 * 1000,
                transitional: {
                    clarifyTimeoutError: true
                },
                httpsAgent: agent
            };

            try {
                let response = await axios(config);
                apiResponse.data = response.data;
                apiResponse.httpcode = response.status;
                apiResponse.response = response.statusText;
                // console.log(response.data);
                // console.log(response.status);
                // console.log(response.statusText);
                // console.log(JSON.stringify(response.data));
                return apiResponse;
            } catch (error) {
                logger("API Error Response", file);
                logger(util.inspect(error), file);
                if ('ETIMEDOUT' !== error.code) {
                    if (error.response) {
                        apiResponse.data = error.response.data;
                        apiResponse.httpcode = error.response.status;
                        apiResponse.response = error.response.statusText;
                        // console.log(error.response.data);
                        // console.log(error.response.status);
                        // console.log(error.response.statusText);
                        return apiResponse;
                    } else {
                        return {
                            httpcode: 0
                        };
                    }
                } else {
                    logger("API Request Time Out", file);
                    return {
                        httpcode: 408
                    };
                }
            }
        }
    }
}

async function writeCalllogs(data, file) {
    const MONGO_HOST = data.mongo.host;
    const MONGO_USER = data.mongo.username;
    const MONGO_DB = data.mongo.dbname;
    const MONGO_PASSWORD = data.mongo.password;
    var mongoData = JSON.stringify(data.Data);
    mongoData = mongoData.replace("'", "'\"'\"'");
    if (data.Data.DateTime) {
        mongoData = mongoData.replace('"<ISODATE>"', 'ISODate("' + data.Data.DateTime + '")');
    } else if (data.Data.CallStartTime) {
        mongoData = mongoData.replace('"<ISODATE>"', 'ISODate("' + data.Data.CallStartTime + '")');
    }
    var command = 'mongo --host ' + MONGO_HOST + ' -u ' + MONGO_USER + ' ' + MONGO_DB + ' -p ' + "'" + MONGO_PASSWORD + "'" + " --eval 'db.calllogs.insert([" + mongoData + "])'";
    const mongoRes = await mongoExec(command, file);
    msg = "Mongo Response ";
    logger(msg, file);
    logger(util.inspect(mongoRes), file);
    let stdout = mongoRes.stdout;
    if (stdout.indexOf('"nInserted" : 1') !== -1) {
        successLog(data, stdout);
        return true;
    } else {
        if (data._TryCount && data._TryCount % 10 == 0 || data._TryCount > 50) {
            var subject = 'Asterisk Call Handler calllog insert failed - ' + data.AppName;
            var message = '<h5>RetryTime :' + data._RetryTime + '</h5><h5>Try Count : ' + data._TryCount +
                '</h5><h5>Method : Mongo</h5><h5>Host : ' + data.mongo.host + '</h5><h5>Calllog : ' +
                JSON.stringify(data.Data) + '</h5><h5>Response : ' + JSON.stringify(mongoRes) + '</h5><h5>File Name : ' + file;
            logger(message, file);
            sendEmail(subject, message);
        }
        return false;
    }
}

async function mongoExec(command, file) {
    try {
        const {
            stdout,
            stderr
        } = await exec(command, { timeout: 10 * 1000 });
        return {
            stdout,
            stderr
        };
    } catch (error) {
        return error;
    }
}

function logger(data, file) {
    var log = moment().format('YYYY-MM-DD HH:mm:ss') + ' ; ' + file + ' ; ' + data;
    const logFile = logStorageFilePath + 'asteriskCallHandlerLog-' + moment().format('YYYYMMDD') + '.log';
    fs.appendFileSync(logFile, log + "\n");
}

function removeFiles(fileName) {
    fs.unlinkSync(storageFilePath + fileName);
}


async function setRetryTime(data, file) {
    if (data._RetryTime) {
        if (data._RetryTime <= moment().format('YYYY-MM-DD HH:mm:ss') && data._TryCount < 50) {
            var array = getOffsetTime();
            var offsetTime = array[data._TryCount];
            var retryOld = 'Retry Old ' + data._RetryTime + ' Retry Seconds ' + offsetTime;
            logger(retryOld, file);
            data._RetryTime = moment(data._RetryTime, 'YYYY-MM-DD HH:mm:ss').add(offsetTime, 'seconds').format('YYYY-MM-DD HH:mm:ss');

            retryNew = " try " + data._TryCount + " new " + data._RetryTime;
            logger(retryNew, file);
            data._TryCount = data._TryCount + 1;
            // @todo check is there any benifit using promise and await over sync functions
            fs.writeFileSync(storageFilePath + file, JSON.stringify(data));
        } else if (data._TryCount >= 50) {
            data._TryCount = data._TryCount + 1;
            fs.writeFileSync(storageFilePath + file, JSON.stringify(data));
            await fs.promises.rename(storageFilePath + file, failedStorageFilePath + file);
        }
    } else {
        data._RetryTime = moment().add(2, 'seconds').format('YYYY-MM-DD HH:mm:ss');
        data._TryCount = 1;
        fs.writeFileSync(storageFilePath + file, JSON.stringify(data));
    }
}

function successLog(data, response) {
    var path = logStorageFilePath + 'asteriskCallHandlerSuccessLog' + moment().format('YYYYMMDD') + '.csv';
    writer = csvWriter({
        sendHeaders: false
    });
    writer.pipe(fs.createWriteStream(path, {
        flags: 'a'
    }));
    writer.write({
        Date: moment().format('YYYY-MM-DD HH:mm:ss'),
        AppName: data.AppName,
        Data: JSON.stringify(data.Data),
        Response: JSON.stringify(response)
    });
    writer.end();
}



function sendEmail(subject, message) {
    // create transporter object with smtp server details
    const transporter = nodemailer.createTransport({
        host: 'email-smtp.ap-south-1.amazonaws.com',
        port: 587,
        auth: {
            user: 'AKIA4BEHSEYUU7BRFU2W',
            pass: 'BE9ddo/8s9LSPIS85032bDflChbT5wb5yip/MN6wWXcc'
        }
    });

    // send email
    try {
        transporter.sendMail({
            from: 'success@waybeo.com',
            to: 'anjima.shaji@waybeo.com',
            subject: subject,
            html: message
        });
    } catch (error) {
        console.log("send mail error");
        logger("send mail error", 'ERROR');
    }

}

function getOffsetTime() {
    var array = {
        1: 2,
        2: 4,
        3: 6,
        4: 8,
        5: 10,
        6: 12,
        7: 14,
        8: 16,
        9: 18,
        10: 20,
        11: 22,
        12: 24,
        13: 26,
        14: 28,
        15: 30,
        16: 32,
        17: 34,
        18: 36,
        19: 38,
        20: 40,
        21: 42,
        22: 44,
        23: 46,
        24: 48,
        25: 50,
        26: 52,
        27: 54,
        28: 56,
        29: 58,
        30: 60,
        31: 100,
        32: 150,
        33: 200,
        34: 250,
        35: 300,
        36: 350,
        37: 400,
        38: 450,
        39: 500,
        40: 550,
        41: 600,
        42: 650,
        43: 1200,
        44: 2400,
        45: 3600,
        46: 18000,
        47: 36000,
        48: 54000,
        49: 72000,
        50: 86400
    };

    return array;

}

// Safe Shutdown
function checkSafeShutdown() {
    try {
        fs.accessSync(global.appPath + '/configs/safe-shutdown', fs.constants.F_OK);
        global.safeShutdown = true;
    } catch (err) {}
}
setInterval(checkSafeShutdown, 5 * 1000);

// process.exit() bug work around
process.on('exit', function() {
    process.kill(process.pid, 'SIGTERM');
});

// Start the application
processCallReport();