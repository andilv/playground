import {
    deadline,
    DeadlineError,
    abortablePromise,
    abortableAsyncIterable,
    readableStreamFromIterable,
    MuxAsyncIterator,

    serve,
    serveTLS,

    resolve as toAbsolute,
    toFileUrl,

    type Server,
    type Response,
    type ServerRequest,

} from './deps.ts';



export type Opts = Partial<{
    auth: string,
    port: Partial<{
        http: number,
        https: number,
    }>,
    timeout: number,
    crt: string,
    key: string,
}>



export async function on_request (
        opts: Opts,
        { signal } = new AbortController() as { readonly signal: AbortSignal },
) {

    const { auth, timeout = 5_000 } = opts;

    const auth_header = auth ?  : undefined;

    return async function (req: ServerRequest) {

        const { url, method, headers } = req;

        if (auth_header && headers.get('Proxy-Authorization') !== auth_header) {
            return req.respond(auth_failure);
        }

        if (method === 'CONNECT') {
            return await on_connect(req, timeout, signal);
        }

        const proxy_req = new Request(url, {
            method,
            headers,
            body: req.body,
            signal,
        });

        try {

            const proxy_res = await deadline(fetch(proxy_req), timeout);

            return await req.respond(proxy_res);

        } catch (err: unknown) {

            if (is_abort_errors(err)) {
                return;
            }

            const cause = err instanceof Error ? err : new Error('unknown', { cause: err });

            return await req.respond({
                status: 502,
                statusText: 'Bad Gateway',
                body: new TextEncoder().encode(cause.message),
            });

        }

    };

}





/** @internal */
export const pre_serves = ({
        http = serve,
        https = serveTLS,
        read_file = Deno.readTextFile,
        info = console.info,
}) => ({

    serve_http: (opts: Opts) => new Promise<Server>((resolve, reject) => {

        const port = port_verify(opts.port?.http) ?? 0;

        if (port < 1) {
            return reject(new Error('no http port'));
        }

        info();

        resolve(http({ port }));

    }),

    async serve_https (opts: Opts) {

        const port = port_verify(opts.port?.https) ?? 0;

        if (port < 1) {
            throw new Error('no https port');
        }

        const { key: opts_key, crt: opts_cert } = opts;

        if (opts_key == null || opts_cert == null) {
            throw new Error('no key or cert file');
        }

        const key = await read_file(opts_key);
        const cert = await read_file(opts_cert);

        info();

        return https({ port, key, cert });

    },

});

const { serve_http, serve_https } = pre_serves({});





async function on_connect (
        req: ServerRequest,
        timeout: number,
        signal: AbortSignal,
) {

    const { url } = req;

    const { hostname, port } = new URL(url);

    const conn = await deadline(Deno.connect({
        hostname,
        port: port_normalize(new URL(url)),
    }), timeout);

    const headers = new Headers();

    headers.set('Connection', 'keep-alive');

    headers.set('Proxy-Agent', 'Deno/1.x');

    await req.respond({
        status: 200,
        headers,
    });

    const remote_conn_reader = readableStreamFromIterable(conn);

    const remote_conn_writer = conn.writable;

    const local_conn_reader = readableStreamFromIterable(req.body);

    const local_conn_writer = req.conn.writable;

    await Promise.race([
        remote_conn_reader.pipeTo(local_conn_writer, { signal }),
        local_conn_reader.pipeTo(remote_conn_writer, { signal }),
    ]);

}





/** @internal */
export function is_abort_errors (e: unknown): e is Error {

    return [

        Deno.errors.BadResource,
        Deno.errors.BrokenPipe,
        Deno.errors.ConnectionReset,
        Deno.errors.Interrupted,

        DeadlineError,

    ].some(clazz => e instanceof clazz);

}





const auth_failure: Response = {
    status: 407,
    statusText: 'Proxy Authentication Required',
    headers: new Headers({ 'Proxy-Authenticate': 'proxy auth' }),
};





/** @internal */
export function port_normalize ({ port, protocol }: URL) {
    return +port || (protocol === 'http:' ? 80 : 443);
}





/** @internal */
export function safe_int ({
        min = Number.MIN_SAFE_INTEGER,
        max = Number.MAX_SAFE_INTEGER,
}) {

    return function (n: unknown): number | undefined {

        if (   typeof n === 'number'
            && Number.isSafeInteger(n)
            && n >= min
            && n <= max
        ) {
            return n;
        }

    };

}





const try_close = (fn: Deno.Closer) => try_catch(() => fn.close());





/** @internal */
export function try_catch <T> (fn: () => T): T | Error {
    try {
        return fn();
    } catch (e: unknown) {
        return e instanceof Error ? e : new Error('unknown', { cause: e });
    }
}





async function* prepend <T> (head: T, tail: AsyncIterable<T>) {
    yield head;
    yield* tail;
}





function catch_iterable_when (predicate: (_: unknown) => boolean) {

    return async function* <T> (iterable: Iterable<T> | AsyncIterable<T>) {

        try {

            yield* iterable;

        } catch (err: unknown) {

            if (predicate(err) === true) {
                return;
            }

            throw err;

        }

    };

}





/** @internal */
export const catch_abortable = catch_iterable_when((err): err is Error => {
    return err instanceof Error && err.name === 'AbortError';
});





/** @internal */
export const port_verify = safe_int({ min: 0, max: 65535 });





/** @internal */
export function pre_tap_catch (error: typeof console.error) {

    return function pre_tap_catch (err?: Error) {
        error(err?.cause ?? (err?.message || err?.name));
        throw err;
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
