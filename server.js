import {readFile, readdir} from 'fs/promises'
import {createWriteStream} from 'fs'
import * as https from "node:https";
import Papa from 'papaparse'

const MAX_SOCKETS = 100;

const download = (url, destPath) => {
    return new Promise((resolve, reject) => {
        console.log(`Downloading: ${url} to ${destPath}`)

        https.get(url, (res) => {
            const filePath = createWriteStream(destPath);

            res.pipe(filePath);
            resolve(true);
        });
    });
};

async function createDownloadRequests(urls) {
    const requests = [];
    let group = [];
    const maxGroups = Math.ceil(urls.length / MAX_SOCKETS);

    let i = 1;

    for (let n = 0; n < maxGroups; n++) {
        group = [];
        const startIndex = n * MAX_SOCKETS;
        const endIndex = n === maxGroups.length - 1
            ? undefined
            : startIndex + MAX_SOCKETS

        for (const url of urls.slice(startIndex, endIndex)) {
            let urlObj = new URL(url);
            let parts = urlObj.pathname.split("/");
            let filename = parts[parts.length - 1];
            // const func = async () => download(url, `${filename}-${i}`);

            group.push(download.bind(null, url, `./downloaded/${filename}-${i}.pdf`));

            i++;
        }

        requests.push(group)
    }

    return requests;
};

async function getRowsFromCsvFile(path, rowName) {
    let links = [];

    if (!path || !rowName) {
        throw new Error('Missing path or rowName attributes')
    }

    const filenames = await readdir(path);

    for (const filename of filenames) {
        const data = await readFile(
            `${path}/${filename}`,
            'utf8'
        );

        if (!data) {
            return [];
        }

        const parsedData = await Papa.parse(data, { header: true });
        links = [...links, ...parsedData.data.map(item => item[rowName])]
    }

    return links;
}

(async () => {
    try {
        const links = await getRowsFromCsvFile('./links/', 'user_invoice_link');
        const requests = await createDownloadRequests(links);
        // console.log(requests);
        let i = 1;

        for (const group of requests) {
            console.log(`process batches ${i}`)
            if (i > 3) {
                try {
                    await Promise.all(group.map(task => task()));

                } catch(e) {
                    throw e;
                }
            }

            i++
        }
    } catch (err) {
        console.error("Caught an error:", err);

        if (err instanceof AggregateError) {
            console.error("Multiple errors:", err.errors); // Logs all errors
        }
    }
})();