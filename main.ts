import {
    on_request,
    serve_http,
    serve_https,
    Opts,
} from './proxy.ts';

import {
    MuxAsyncIterator,
    catch_abortable,
} from './deps.ts';


export async function main (
        opts: Opts,
        { signal } = new AbortController() as { readonly signal: AbortSignal },
) {

    const handle = await on_request(opts);

    const services = await Promise.allSettled([
        serve_http(opts),
        serve_https(opts),
    ]);

    if (services.every(settling.rejected)) {

        const cause = [
            '',
            ...services.filter(settling.rejected).map(r => r.reason),
        ].join('\n');

        throw new Error('program exited', { cause });

    }

    const fulfilled = services.filter(settling.fulfilled).map(r => r.value);

    try {

        const mux = new MuxAsyncIterator<ServerRequest>();

        fulfilled.forEach(it => mux.add(it));

        const listener = catch_abortable(abortableAsyncIterable(mux, signal));

        for await (const conn of listener) {
            handle(conn);
        }

    } finally {

        fulfilled.forEach(try_close);

    }

}


const settling = {

    fulfilled <T> (
        result  : PromiseSettledResult<T>,
    ) : result is PromiseFulfilledResult<T> {
        return result.status === 'fulfilled';
    },

    rejected <T> (
        result  : PromiseSettledResult<T>,
    ) : result is PromiseRejectedResult {
        return result.status === 'rejected';
    },

} as const;


const try_close = (fn: Deno.Closer) => try_catch(() => fn.close());


export function try_catch <T> (fn: () => T): T | Error {
    try {
        return fn();
    } catch (e: unknown) {
        return e instanceof Error ? e : new Error('unknown', { cause: e });
    }
}
