// Generated public model modules contain pure calculations only, no credentials,
// database access, connector code or server authentication code.
import {mkdir,copyFile} from 'node:fs/promises';
await mkdir(new URL('../public/model/',import.meta.url),{recursive:true});
for(const name of ['analysis','decisions','constants','weights','news-format'])await copyFile(new URL(`../src/${name}.js`,import.meta.url),new URL(`../public/model/${name}.js`,import.meta.url));
console.log('Built five shared browser calculation/format modules.');
