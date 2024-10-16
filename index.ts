import type { StorageAdapterInterface } from '@automerge/automerge-repo/slim';
import * as Comlink from 'comlink';
import { OPFSStorageAdapter } from './opfs';

type Constructor<K> = { new (): K };
const OPFSWorker = Comlink.wrap<Constructor<OPFSStorageAdapter>>(
	new Worker(new URL('./worker.ts', import.meta.url), { type: 'module' })
);

export class OPFSWorkerStorageAdapter implements StorageAdapterInterface {
	private worker;

	constructor(directory: string = 'automerge') {
		this.worker = new OPFSWorker(directory);
	}

	private workerProxy<T extends keyof StorageAdapterInterface>(method: T): StorageAdapterInterface[T] {
		return (...args: any[]) => this.worker.then(w => (w[method] as any)(...args));
	}

	load = this.workerProxy('load');
	save = this.workerProxy('save');
	remove = this.workerProxy('remove');
	loadRange = this.workerProxy('loadRange');
	removeRange = this.workerProxy('removeRange');
}

export default OPFSWorkerStorageAdapter;
