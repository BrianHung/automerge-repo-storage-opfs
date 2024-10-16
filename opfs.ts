import type { Chunk, StorageAdapterInterface, StorageKey } from '@automerge/automerge-repo/slim';

export class OPFSStorageAdapter implements StorageAdapterInterface {
	private directory: Promise<FileSystemDirectoryHandle>;
	private cache: { [key: string]: Uint8Array } = {};

	private fileHandleCache: { [key: string]: FileSystemFileHandle } = {};
	private directoryHandleCache: { [key: string]: FileSystemDirectoryHandle } = {};

	constructor(directory: string = 'automerge') {
		this.directory = navigator.storage.getDirectory().then(dir => dir.getDirectoryHandle(directory, { create: true }));
	}

	async load(keyArray: StorageKey): Promise<Uint8Array | undefined> {
		const key = getKey(keyArray);
		if (this.cache[key]) return this.cache[key];

		const path = getFilePath(keyArray);
		const handle = await this.getFileHandle(path);
		const file = await handle!.getFile();
		if (file.size) {
			const bytes = new Uint8Array(await file.arrayBuffer());
			this.cache[key] = bytes;
			return bytes;
		} else {
			return undefined;
		}
	}

	async save(keyArray: StorageKey, binary: Uint8Array): Promise<void> {
		const key = getKey(keyArray);
		this.cache[key] = binary;
		const path = getFilePath(keyArray);
		const fileHandle = await this.getFileHandle(path);
		// Only available in WebWorker.
		if ('createSyncAccessHandle' in fileHandle) {
			const handle = await fileHandle.createSyncAccessHandle({ mode: 'readwrite-unsafe' });
			await handle.write(binary);
			await handle.flush();
			await handle.close();
		} else {
			const writable = await fileHandle!.createWritable({ keepExistingData: false });
			await writable.write(binary);
			await writable.close();
		}
	}

	async remove(keyArray: StorageKey): Promise<void> {
		const key = getKey(keyArray);
		delete this.cache[key];
		const path = getFilePath(keyArray);
		const dirPath = path.slice(0, -1);
		const fileName = path[path.length - 1];
		const handle = await this.getDirectoryHandle(dirPath);
		await handle.removeEntry(fileName, { recursive: true });
	}

	async loadRange(keyPrefix: StorageKey): Promise<Chunk[]> {
		const path = getFilePath(keyPrefix);

		const chunks: Chunk[] = [];
		const skip = new Set<string>();
		this.cachedKeys(keyPrefix).forEach(key => {
			skip.add(key);
			chunks.push({
				key: getKeyArray(key.split('-')),
				data: this.cache[key],
			});
		});

		const dir = await this.getDirectoryHandle(path);
		const handles = await getFileHandlesRecursively(dir, path);
		await Promise.all(
			handles.map(async ({ key, handle }) => {
				const fileCacheKey = getKey(key);
				if (skip.has(fileCacheKey)) return;
				const bytes = new Uint8Array(await (await handle.getFile()).arrayBuffer());
				this.cache[fileCacheKey] = bytes;
				chunks.push({
					key: getKeyArray(key),
					data: bytes,
				});
			})
		);

		return chunks;
	}
	async removeRange(keyPrefix: StorageKey): Promise<void> {
		this.cachedKeys(keyPrefix).forEach(key => delete this.cache[key]);
		const path = getFilePath(keyPrefix);
		const parent = await this.getDirectoryHandle(path.slice(0, -1));
		await parent.removeEntry(path[path.length - 1], { recursive: true });
	}

	private cachedKeys(keyPrefix: string[]): string[] {
		const cacheKeyPrefixString = getKey(keyPrefix);
		return Object.keys(this.cache).filter(key => key.startsWith(cacheKeyPrefixString));
	}

	private async getFileHandle(path: string[]) {
		const key = getKey(path);
		if (this.fileHandleCache[key]) return this.fileHandleCache[key];
		const dirPath = path.slice(0, -1);
		const fileName = path[path.length - 1];
		const dirHandle = await this.getDirectoryHandle(dirPath);
		const handle = await dirHandle.getFileHandle(fileName, { create: true });
		this.fileHandleCache[key] = handle;
		return handle;
	}

	private async getDirectoryHandle(path: string[]) {
		const key = getKey(path);
		if (this.directoryHandleCache[key]) return this.directoryHandleCache[key];

		let dir = await this.directory;
		for (const part of path) {
			dir = await dir.getDirectoryHandle(part, { create: true });
		}
		this.directoryHandleCache[key] = dir;
		return dir;
	}
}

export default OPFSStorageAdapter;

function getFilePath(keyArray: string[]): string[] {
	const [firstKey, ...remainingKeys] = keyArray;
	return [firstKey.slice(0, 2), firstKey.slice(2), ...remainingKeys];
}

function getKeyArray(path: string[]): StorageKey {
	const [prefix, firstKey, ...remainingKeys] = path;
	return [prefix + firstKey, ...remainingKeys];
}

const getKey = (key: StorageKey): string => key.join('-');

async function getFileHandlesRecursively(dir: FileSystemDirectoryHandle, prefix: string[]) {
	let handles: { key: string[]; handle: FileSystemFileHandle }[] = [];

	for await (const [name, handle] of dir.entries()) {
		let next = prefix.concat(name);
		if (handle.kind == 'directory') {
			handles = handles.concat(await getFileHandlesRecursively(handle as FileSystemDirectoryHandle, next));
		} else {
			handles.push({
				key: next,
				handle: handle as FileSystemFileHandle,
			});
		}
	}

	return handles;
}
