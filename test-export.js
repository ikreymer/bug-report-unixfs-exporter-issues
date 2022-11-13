import test from "ava";

import fs from "fs";
import { MemoryBlockstore } from "blockstore-core";

import { importer } from 'ipfs-unixfs-importer';
import { exporter } from 'ipfs-unixfs-exporter';

import { CID } from 'multiformats/cid';
import { sha256 } from 'multiformats/hashes/sha2';
import { UnixFS } from "ipfs-unixfs";
import * as dagPb from '@ipld/dag-pb';

const offsets = [ 265, 2527, 57313, 63504 ];
const store = new MemoryBlockstore();

const cids = [];
const sizes = {};

async function importRange(filename, start, end, opts = {}) {
  const source = [{content: fs.createReadStream(filename, {start, end})}];

  let last;

  for await (const entry of importer(source, store, opts)) {
    last = entry;
  }

  cids.push(last.cid);
  sizes[last.cid] = last.size;
}

const filename = "iana.warc";

async function readIter(iter) {
  const bufs = [];
  for await (const chunk of iter) {
    bufs.push(chunk);
  }
  return Buffer.concat(bufs);
}

async function main(t) {
  const origFile = await readIter(fs.createReadStream(filename, {start: 0, end: 63503}));
  console.log("orig", origFile.length);

  await importRange(filename, 0, 264);
  await importRange(filename, 265, 2526);
  await importRange(filename, 2527, 57312);
  await importRange(filename, 57313, 63503);

  const cid = await concat(cids);

  const entry = await exporter(cid, store);

  const exported = await readIter(entry.content());

  console.log("exported", exported.length);

  //console.log("equals", JSON.stringify(Array.from(exported)) === JSON.stringify(Array.from(origFile)));
  t.deepEqual(exported, origFile);
}

export async function concat(cids) {
  const node = new UnixFS({ type: "file" });

  const Links = await Promise.all(
    cids.map(async (cid) => {
      const Tsize = sizes[cid];
      return {
        Name: "",
        Hash: cid,
        Tsize,
      };
    })
  );

  Links.map(({ Tsize }) => node.addBlockSize(Tsize));

  const Data = node.marshal();

  return await putBlock({Data, Links});
}

async function putBlock(node) {
  const bytes = dagPb.encode(dagPb.prepare(node));
  const hash = await sha256.digest(bytes);
  const cid = CID.create(1, dagPb.code, hash);
  store.put(cid, bytes);
  return cid;
}

test("export matches original file", async t => {
  await main(t);
});
